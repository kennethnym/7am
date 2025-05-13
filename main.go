package main

import (
	"context"
	"database/sql"
	"embed"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/SherClockHolmes/webpush-go"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"google.golang.org/genai"
	"html/template"
	"log"
	"log/slog"
	"mime"
	_ "modernc.org/sqlite"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

type location struct {
	tz          *time.Location
	lat         float32
	lon         float32
	ianaName    string
	displayName string
}

// pageTemplate stores all pre-compiled HTML templates for the application
type pageTemplate struct {
	summary *template.Template
}

// summaryTemplateData stores template data for summary.html
type summaryTemplateData struct {
	Summary      string
	Location     string
	LocationName string
}

// updateSubscription is the request body for creating/updating registration
type updateSubscription struct {
	Subscription    webpush.Subscription `json:"subscription"`
	Locations       []string             `json:"locations"`
	RemoveLocations []string             `json:"removeLocations"`
}

// registeredSubscription represents a registered webpush subscription.
type registeredSubscription struct {
	ID           uuid.UUID             `json:"id"`
	Subscription *webpush.Subscription `json:"-"`
	Locations    []string              `json:"locations"`
}

type webpushNotificationPayload struct {
	Summary  string `json:"summary"`
	Location string `json:"location"`
}

type metAPIData struct {
	Properties struct {
		TimeSeries []map[string]any `json:"timeseries"`
	} `json:"properties"`
}

type updateSummaryOptions struct {
	locKey     string
	location   *location
	pushUpdate bool
}

type state struct {
	ctx             context.Context
	metAPIUserAgent string
	genai           *genai.Client
	template        pageTemplate

	db      *sql.DB
	dbMutex sync.Mutex

	// summaries maps location keys to their latest weather summary
	summaries sync.Map
	// summaryChans stores a map of location key to the corresponding summary channel
	// which is used to track summary updates
	summaryChans map[string]chan string

	// subscriptions maps location keys to the list of registered subscriptions
	// that are subscribed to updates for the location
	subscriptions map[string][]*registeredSubscription
	// subscriptionsMutex syncs writes to subscriptions
	subscriptionsMutex sync.Mutex

	vapidSubject string
	// vapidPublicKey is the base64 url encoded VAPID public key
	vapidPublicKey string
	// vapidPrivateKey is the base64 url encoded VAPID private key
	vapidPrivateKey string
}

//go:embed web
var webDir embed.FS

var envKeys = []string{"GEMINI_API_KEY", "MET_API_USER_AGENT", "VAPID_SUBJECT", "VAPID_PRIVATE_KEY_BASE64", "VAPID_PUBLIC_KEY_BASE64"}

//go:embed prompt.txt
var prompt string

var supportedLocations = map[string]*location{
	"london":    {nil, 51.507351, -0.127758, "Europe/London", "London"},
	"sf":        {nil, 37.774929, -122.419418, "America/Los_Angeles", "San Francisco"},
	"sj":        {nil, 37.338207, -121.886330, "America/Los_Angeles", "San Jose"},
	"la":        {nil, 34.052235, -118.243683, "America/Los_Angeles", "Los Angeles"},
	"nyc":       {nil, 40.712776, -74.005974, "America/New_York", "New York City"},
	"tokyo":     {nil, 35.689487, 139.691711, "Asia/Tokyo", "Tokyo"},
	"singapore": {nil, 1.290270, 103.851959, "Asia/Singapore", "Singapore"},
	"warsaw":    {nil, 52.229675, 21.012230, "Europe/Warsaw", "Warsaw"},
	"zurich":    {nil, 47.369019, 8.538030, "Europe/Zurich", "Zurich"},
	"berlin":    {nil, 52.520008, 13.404954, "Europe/Berlin", "Berlin"},
	"dubai":     {nil, 25.204849, 55.270782, "Asia/Dubai", "Dubai"},
	"paris":     {nil, 48.864716, 2.349014, "Europe/Paris", "Paris"},
	"stockholm": {nil, 59.329323, 18.068581, "Europe/Stockholm", "Stockholm"},
	"amsterdam": {nil, 52.370216, 4.895168, "Europe/Amsterdam", "Amsterdam"},
}

func main() {
	port := flag.Int("port", 8080, "the port that the server should listen on")
	genKeys := flag.Bool("generate-vapid-keys", false, "generate a new vapid key pair, which will be outputted to stdout.")

	flag.Parse()

	if *genKeys {
		generateKeys()
	} else if err := startServer(*port); err != nil {
		log.Fatal(err)
	}
}

func startServer(port int) error {
	slog.Info("starting 7am...")

	err := loadTimeZones()
	if err != nil {
		return err
	}

	_ = godotenv.Load()
	err = checkEnv()
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get cwd: %w", err)
	}

	p := filepath.Join(wd, "data")
	err = os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create data directory at %v: %w", p, err)
	}
	slog.Info("data directory created", "path", p)

	db, err := initDB()
	if err != nil {
		return fmt.Errorf("failed to initialize db: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genaiClient, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  os.Getenv("GEMINI_API_KEY"),
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize gemini client: %w\n", err)
	}

	summaryHTML, _ := webDir.ReadFile("web/summary.html")
	summaryPageTemplate, _ := template.New("summary.html").Parse(string(summaryHTML))

	state := state{
		ctx:             ctx,
		metAPIUserAgent: os.Getenv("MET_API_USER_AGENT"),
		template: pageTemplate{
			summary: summaryPageTemplate,
		},
		summaries:    sync.Map{},
		summaryChans: map[string]chan string{},
		genai:        genaiClient,

		db:      db,
		dbMutex: sync.Mutex{},

		subscriptions: map[string][]*registeredSubscription{},

		vapidSubject:    os.Getenv("VAPID_SUBJECT"),
		vapidPublicKey:  os.Getenv("VAPID_PUBLIC_KEY_BASE64"),
		vapidPrivateKey: os.Getenv("VAPID_PRIVATE_KEY_BASE64"),
	}

	fetchInitialSummaries(&state)

	var schedulers []gocron.Scheduler

	// schedule periodic updates of weather summary for each supported location
	for locKey, loc := range supportedLocations {
		s, err := gocron.NewScheduler(gocron.WithLocation(loc.tz))
		if err != nil {
			return fmt.Errorf("failed to create gocron scheduler for %v: %w", locKey, err)
		}

		_, err = s.NewJob(
			gocron.DailyJob(1, gocron.NewAtTimes(gocron.NewAtTime(7, 0, 0))),
			gocron.NewTask(func(ctx context.Context) {
				updateSummary(ctx, &state, updateSummaryOptions{
					locKey:     locKey,
					location:   loc,
					pushUpdate: true,
				})
			}),
		)
		if err != nil {
			return fmt.Errorf("failed to scheduel gocron job for %v: %w", locKey, err)
		}

		schedulers = append(schedulers, s)
		c := make(chan string)

		state.subscriptions[locKey] = []*registeredSubscription{}
		state.summaryChans[locKey] = c

		// listen for summary updates, and publish updates to all update subscribers via web push
		go listenForSummaryUpdates(&state, locKey, c)

		s.Start()

		slog.Info("update job scheduled", "location", locKey)
	}

	err = loadSubscriptions(&state)
	if err != nil {
		return fmt.Errorf("failed to load existing subscriptions: %w", err)
	}

	http.HandleFunc("/", handleHTTPRequest(&state))

	slog.Info("server starting", "port", port)

	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		return fmt.Errorf("failed to start http server: %w", err)
	}

	for _, s := range schedulers {
		s.Shutdown()
	}

	slog.Info("7am shut down")

	return nil
}

func generateKeys() {
	priv, pub, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("all keys are base64 url encoded.")
	fmt.Printf("public key: %v\n", pub)
	fmt.Printf("private key: %v\n", priv)
}

func checkEnv() error {
	var missing []string
	for _, k := range envKeys {
		v := os.Getenv(k)
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing env: %v", strings.Join(missing, ", "))
	}
	return nil
}

func handleHTTPRequest(state *state) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		path := strings.TrimPrefix(request.URL.Path, "/")

		if path == "" {
			if request.Method == "" || request.Method == "GET" {
				index, _ := webDir.ReadFile("web/index.html")
				writer.Write(index)
			} else {
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else if path == "instructions" {
			f, _ := webDir.ReadFile("web/instructions.html")
			writer.Write(f)
		} else if path == "vapid" {
			if request.Method == "" || request.Method == "GET" {
				writer.Write([]byte(state.vapidPublicKey))
			} else {
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else if strings.HasPrefix(path, "registrations") {
			if path == "registrations" && request.Method == "POST" {
				defer request.Body.Close()

				update := updateSubscription{}
				err := json.NewDecoder(request.Body).Decode(&update)
				if err != nil {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				reg, err := registerSubscription(state, &update)
				if err != nil {
					slog.Error("web push subscription registration failed", "error", err)
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				err = json.NewEncoder(writer).Encode(reg)
				if err != nil {
					writer.WriteHeader(http.StatusBadRequest)
				} else {
					slog.Info("new web push registration", "id", reg.ID)
				}
			} else if request.Method == "PATCH" || request.Method == "DELETE" {
				parts := strings.Split(path, "/")
				if len(parts) < 2 {
					writer.WriteHeader(http.StatusMethodNotAllowed)
					return
				}

				regID, err := uuid.Parse(parts[1])
				if err != nil {
					writer.WriteHeader(http.StatusNotFound)
					return
				}

				switch request.Method {
				case "PATCH":
					defer request.Body.Close()

					update := updateSubscription{}
					err = json.NewDecoder(request.Body).Decode(&update)
					if err != nil {
						writer.WriteHeader(http.StatusBadRequest)
						return
					}

					reg, err := updateRegisteredSubscription(state, regID, &update)
					if err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							writer.WriteHeader(http.StatusNotFound)
						} else {
							writer.WriteHeader(http.StatusInternalServerError)
						}
					} else {
						json.NewEncoder(writer).Encode(reg)
						slog.Info("web push registration updated", "id", reg.ID, "locations", strings.Join(reg.Locations, ","))
					}

				case "DELETE":
					err = deleteSubscription(state, regID)
					if err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							writer.WriteHeader(http.StatusNotFound)
						} else {
							writer.WriteHeader(http.StatusInternalServerError)
						}
					} else {
						writer.WriteHeader(http.StatusNoContent)
						slog.Info("web push registration deleted", "id", regID)
					}
				}

			} else {
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}

		} else {
			if request.Method != "" && request.Method != "GET" {
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			summary, ok := state.summaries.Load(path)
			if ok {
				loc := supportedLocations[path]
				state.template.summary.Execute(writer, summaryTemplateData{summary.(string), path, loc.displayName})
			} else {
				f, err := webDir.ReadFile("web/" + path)
				if err != nil {
					writer.WriteHeader(http.StatusNotFound)
				} else {
					m := mime.TypeByExtension(filepath.Ext(path))
					if m != "" {
						writer.Header().Set("Content-Type", m)
					}
					writer.Write(f)
				}
			}
		}
	}
}

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:data/data.sqlite")
	if err != nil {
		log.Fatalln("failed to initialize database")
	}

	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;

		CREATE TABLE IF NOT EXISTS subscriptions(
			id TEXT PRIMARY KEY,
			locations TEXT NOT NULL,
			subscription_json TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS summaries(
			location TEXT PRIMARY KEY,
			summary TEXT NOT NULL
		);
	`)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func loadSubscriptions(state *state) error {
	rows, err := state.db.Query(`SELECT id, locations, subscription_json FROM subscriptions;`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var locations string
		var j string

		err := rows.Scan(&id, &locations, &j)
		if err != nil {
			slog.Warn("unable to load a subscription", "error", err)
			continue
		}

		s := webpush.Subscription{}
		err = json.Unmarshal([]byte(j), &s)
		if err != nil {
			slog.Warn("invalid web push subscription json encountered", "id", id, "error", err)
			continue
		}

		reg := &registeredSubscription{
			ID:           uuid.MustParse(id),
			Locations:    strings.Split(locations, ","),
			Subscription: &s,
		}

		for _, l := range reg.Locations {
			state.subscriptions[l] = append(state.subscriptions[l], reg)
		}
	}

	return nil
}

func updateRegisteredSubscription(state *state, id uuid.UUID, update *updateSubscription) (*registeredSubscription, error) {
	j, err := json.Marshal(update.Subscription)
	if err != nil {
		return nil, err
	}

	rows, err := state.db.Query("SELECT locations FROM subscriptions WHERE id = ?", id)
	if err != nil {
		return nil, err
	}

	rows.Next()

	var locStr string
	err = rows.Scan(&locStr)
	if err != nil {
		return nil, err
	}

	rows.Close()

	// not very proud of this one
	// ideally the list of locations should be stored in a separate table
	// but since the list is very small, and im too lazy to bring in a separate table
	// this should be fine for now
	locs := strings.Split(locStr, ",")
	locs = append(locs, update.Locations...)
	locs = slices.DeleteFunc(locs, func(l string) bool {
		return slices.Contains(update.RemoveLocations, l)
	})
	locs = slices.Compact(locs)

	_, err = state.db.Exec(
		"UPDATE subscriptions SET subscription_json = ?, locations = ? WHERE id = ?",
		string(j), strings.Join(locs, ","), id,
	)
	if err != nil {
		return nil, err
	}

	reg := &registeredSubscription{
		ID:           id,
		Subscription: &update.Subscription,
		Locations:    locs,
	}

	state.subscriptionsMutex.Lock()
	for _, l := range update.Locations {
		state.subscriptions[l] = append(state.subscriptions[l], reg)
	}
	for _, l := range update.RemoveLocations {
		state.subscriptions[l] = slices.DeleteFunc(state.subscriptions[l], func(s *registeredSubscription) bool {
			return s.ID == reg.ID
		})
	}
	state.subscriptionsMutex.Unlock()

	return reg, nil
}

func registerSubscription(state *state, sub *updateSubscription) (*registeredSubscription, error) {
	j, err := json.Marshal(sub.Subscription)
	if err != nil {
		return nil, fmt.Errorf("invalid web push subscription object: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("unable to generate id for subscription: %w", err)
	}

	locs := slices.Compact(sub.Locations)

	_, err = state.db.Exec(
		"INSERT INTO subscriptions (id, locations, subscription_json) VALUES (?, ?, ?);",
		id, strings.Join(locs, ","), string(j),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to insert into subscriptions table: %w", err)
	}

	reg := registeredSubscription{
		ID:           id,
		Subscription: &sub.Subscription,
		Locations:    locs,
	}

	state.subscriptionsMutex.Lock()
	for _, l := range sub.Locations {
		state.subscriptions[l] = append(state.subscriptions[l], &reg)
	}
	state.subscriptionsMutex.Unlock()

	return &reg, nil
}

func deleteSubscription(state *state, regID uuid.UUID) error {
	_, err := state.db.Exec("DELETE FROM subscriptions WHERE id = ?", regID)
	return err
}

func loadTimeZones() error {
	for locKey, loc := range supportedLocations {
		tz, err := time.LoadLocation(loc.ianaName)
		if err != nil {
			return fmt.Errorf("failed to load time zone for %v: %w", locKey, err)
		}
		loc.tz = tz
	}
	return nil
}

func fetchInitialSummaries(state *state) {
	var wg sync.WaitGroup
	for locKey, loc := range supportedLocations {
		wg.Add(1)
		go func() {
			ctx, cancel := context.WithCancel(state.ctx)
			defer cancel()

			defer wg.Done()

			summary := ""
			rows, err := state.db.QueryContext(ctx, "SELECT summary FROM summaries WHERE location = ?", locKey)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				slog.Warn("unable to get cached weather summary", "location", locKey, "error", err)
			} else if err == nil {
				defer rows.Close()
				ok := rows.Next()
				if ok {
					err = rows.Scan(&summary)
					if err != nil {
						slog.Warn("unable to get cached weather summary", "location", locKey, "error", err)
					}
				}
			}

			if summary == "" {
				updateSummary(state.ctx, state, updateSummaryOptions{
					locKey:     locKey,
					location:   loc,
					pushUpdate: false,
				})
			} else {
				state.summaries.Store(locKey, summary)
			}
		}()
	}
	wg.Wait()
}

func updateSummary(ctx context.Context, state *state, opts updateSummaryOptions) {
	locKey := opts.locKey
	loc := opts.location

	slog.Info("updating weather summary", "location", locKey)

	today := time.Now().In(loc.tz)

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=%v&lon=%v", loc.lat, loc.lon), nil)
	if err != nil {
		slog.Error("failed to query weather data", "location", locKey, "error", err)
		return
	}
	req.Header.Set("User-Agent", state.metAPIUserAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("failed to query weather data", "location", locKey, "error", err)
		return
	}

	defer resp.Body.Close()

	data := metAPIData{}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		slog.Error("failed to decode received weather data", "location", locKey, "error", err)
		return
	}

	y, m, d := today.Date()

	t := slices.DeleteFunc(data.Properties.TimeSeries, func(series map[string]any) bool {
		if ts, ok := series["time"].(string); ok {
			t, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				return false
			}
			ty, tm, td := t.In(loc.tz).Date()
			return !(y == ty && m == tm && d == td)
		}
		return false
	})

	b, err := json.Marshal(t)
	if err != nil {
		slog.Error("failed to marshal processed time series data", "location", locKey, "error", err)
		return
	}

	weatherJSON := string(b)

	result, err := state.genai.Models.GenerateContent(ctx, "gemini-2.0-flash", []*genai.Content{{
		Parts: []*genai.Part{
			{Text: fmt.Sprintf(prompt, today.Format("2006-02-01"), loc.displayName, loc.displayName)},
			{Text: weatherJSON},
		},
	}}, nil)
	if err != nil {
		slog.Error("failed to generate weather summary", "location", locKey, "error", err)
		return
	}

	summary := result.Text()

	state.dbMutex.Lock()
	_, err = state.db.ExecContext(ctx, "INSERT OR REPLACE INTO summaries (location, summary) VALUES (?, ?)", locKey, summary)
	if err != nil {
		slog.Warn("unable to cache generated weather summary to db", "location", locKey, "error", err)
	}
	state.dbMutex.Unlock()

	state.summaries.Store(locKey, summary)

	if opts.pushUpdate {
		c := state.summaryChans[locKey]
		if len(state.subscriptions[locKey]) > 0 {
			c <- summary
		}
	}

	slog.Info("updated weather summary", "location", locKey)
}

func listenForSummaryUpdates(state *state, locKey string, c <-chan string) {
	opts := webpush.Options{
		Subscriber:      state.vapidSubject,
		VAPIDPublicKey:  state.vapidPublicKey,
		VAPIDPrivateKey: state.vapidPrivateKey,
		TTL:             30,
	}

	for {
		select {
		case summary := <-c:
			payload := webpushNotificationPayload{
				Summary:  summary,
				Location: locKey,
			}
			b, err := json.Marshal(&payload)
			if err != nil {
				slog.Error("failed to create web push notification payload", "location", locKey, "error", err)
				continue
			}

			subs := state.subscriptions[locKey]

			slog.Info("pushing weather summary to subscribers", "count", len(subs), "location", locKey)

			var wg sync.WaitGroup
			for _, sub := range subs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := webpush.SendNotificationWithContext(state.ctx, b, sub.Subscription, &opts)
					if err != nil {
						slog.Warn("unable to send web push to subscription", "id", sub.ID, "location", locKey, "error", err)
					}
				}()
			}

			wg.Wait()

			slog.Info("pushed weather summary to subscribers", "count", len(subs), "location", locKey)

		case <-state.ctx.Done():
			return
		}
	}
}
