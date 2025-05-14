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
	"io"
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

type state struct {
	ctx             context.Context
	db              *sql.DB
	metAPIUserAgent string
	genai           *genai.Client
	template        pageTemplate

	usePlaceholder bool

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

var placeholderWeather = map[string]string{
	"london": "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T08:00:00+01:00\",\"EffectiveEpochDate\":1746946800,\"Severity\":4,\"Text\":\"Pleasant Sunday\",\"Category\":\"mild\",\"EndDate\":null,\"EndEpochDate\":null,\"MobileLink\":\"http://www.accuweather.com/en/gb/london/ec4a-2/daily-weather-forecast/328328?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/gb/london/ec4a-2/daily-weather-forecast/328328?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00+01:00\",\"EpochDate\":1746856800,\"Sun\":{\"Rise\":\"2025-05-10T05:17:00+01:00\",\"EpochRise\":1746850620,\"Set\":\"2025-05-10T20:38:00+01:00\",\"EpochSet\":1746905880},\"Moon\":{\"Rise\":\"2025-05-10T18:40:00+01:00\",\"EpochRise\":1746898800,\"Set\":\"2025-05-11T04:21:00+01:00\",\"EpochSet\":1746933660,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":69,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":70,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":12.8,\"DegreeDaySummary\":{\"Heating\":{\"Value\":5,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":32767,\"Category\":\"High\",\"CategoryValue\":3},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":7,\"Category\":\"High\",\"CategoryValue\":3}],\"Day\":{\"Icon\":1,\"IconPhrase\":\"Sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Sunshine, breezy and pleasant\",\"LongPhrase\":\"Breezy and pleasant with sunshine\",\"PrecipitationProbability\":1,\"ThunderstormProbability\":0,\"RainProbability\":1,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":13.8,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":97,\"Localized\":\"E\",\"English\":\"E\"}},\"WindGust\":{\"Speed\":{\"Value\":32.2,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":88,\"Localized\":\"E\",\"English\":\"E\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":6,\"Evapotranspiration\":{\"Value\":0.18,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":7999.7,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":27,\"Maximum\":71,\"Average\":39},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":61,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":38,\"IconPhrase\":\"Mostly cloudy\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Increasing cloudiness\",\"LongPhrase\":\"Increasing cloudiness\",\"PrecipitationProbability\":1,\"ThunderstormProbability\":0,\"RainProbability\":1,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":6.9,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":69,\"Localized\":\"ENE\",\"English\":\"ENE\"}},\"WindGust\":{\"Speed\":{\"Value\":20.7,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":106,\"Localized\":\"ESE\",\"English\":\"ESE\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":32,\"Evapotranspiration\":{\"Value\":0.02,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":155.7,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":44,\"Maximum\":82,\"Average\":67},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":54,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/gb/london/ec4a-2/daily-weather-forecast/328328?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/gb/london/ec4a-2/daily-weather-forecast/328328?day=1&lang=en-us\"}]}",
	"sf":     "{\"Headline\":{\"EffectiveDate\":\"2025-05-10T08:00:00-07:00\",\"EffectiveEpochDate\":1746889200,\"Severity\":4,\"Text\":\"Pleasant today\",\"Category\":\"mild\",\"EndDate\":null,\"EndEpochDate\":null,\"MobileLink\":\"http://www.accuweather.com/en/us/san-francisco-ca/94103/daily-weather-forecast/347629?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/san-francisco-ca/94103/daily-weather-forecast/347629?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00-07:00\",\"EpochDate\":1746885600,\"Sun\":{\"Rise\":\"2025-05-10T06:04:00-07:00\",\"EpochRise\":1746882240,\"Set\":\"2025-05-10T20:08:00-07:00\",\"EpochSet\":1746932880},\"Moon\":{\"Rise\":\"2025-05-10T18:41:00-07:00\",\"EpochRise\":1746927660,\"Set\":\"2025-05-11T05:13:00-07:00\",\"EpochSet\":1746965580,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":71,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":73,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":67,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":10.2,\"DegreeDaySummary\":{\"Heating\":{\"Value\":3,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":49,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Particle Pollution\"},{\"Name\":\"Grass\",\"Value\":12,\"Category\":\"Moderate\",\"CategoryValue\":2},{\"Name\":\"Mold\",\"Value\":3250,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":5,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":7,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":10,\"Category\":\"Very High\",\"CategoryValue\":4}],\"Day\":{\"Icon\":2,\"IconPhrase\":\"Mostly sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Sunshine and pleasant\",\"LongPhrase\":\"Sunny to partly cloudy and pleasant\",\"PrecipitationProbability\":1,\"ThunderstormProbability\":0,\"RainProbability\":1,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":11.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":254,\"Localized\":\"WSW\",\"English\":\"WSW\"}},\"WindGust\":{\"Speed\":{\"Value\":29.9,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":257,\"Localized\":\"WSW\",\"English\":\"WSW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":28,\"Evapotranspiration\":{\"Value\":0.15,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":8489.7,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":51,\"Maximum\":91,\"Average\":65},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":61,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":56,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":62,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":35,\"IconPhrase\":\"Partly cloudy\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Partly cloudy\",\"LongPhrase\":\"Partly cloudy\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":11.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":268,\"Localized\":\"W\",\"English\":\"W\"}},\"WindGust\":{\"Speed\":{\"Value\":19.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":264,\"Localized\":\"W\",\"English\":\"W\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":45,\"Evapotranspiration\":{\"Value\":0.02,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":190.9,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":57,\"Maximum\":84,\"Average\":73},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":54,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":55,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/us/san-francisco-ca/94103/daily-weather-forecast/347629?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/san-francisco-ca/94103/daily-weather-forecast/347629?day=1&lang=en-us\"}]}",
	"sj":     "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T08:00:00-07:00\",\"EffectiveEpochDate\":1746975600,\"Severity\":4,\"Text\":\"Pleasant tomorrow\",\"Category\":\"mild\",\"EndDate\":null,\"EndEpochDate\":null,\"MobileLink\":\"http://www.accuweather.com/en/us/san-jose-ca/95110/daily-weather-forecast/347630?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/san-jose-ca/95110/daily-weather-forecast/347630?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00-07:00\",\"EpochDate\":1746885600,\"Sun\":{\"Rise\":\"2025-05-10T06:03:00-07:00\",\"EpochRise\":1746882180,\"Set\":\"2025-05-10T20:05:00-07:00\",\"EpochSet\":1746932700},\"Moon\":{\"Rise\":\"2025-05-10T18:38:00-07:00\",\"EpochRise\":1746927480,\"Set\":\"2025-05-11T05:11:00-07:00\",\"EpochSet\":1746965460,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":84,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":87,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Warm\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":81,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":12.7,\"DegreeDaySummary\":{\"Heating\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":4,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":70,\"Category\":\"Moderate\",\"CategoryValue\":2,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":12,\"Category\":\"Moderate\",\"CategoryValue\":2},{\"Name\":\"Mold\",\"Value\":3250,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":5,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":7,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":10,\"Category\":\"Very High\",\"CategoryValue\":4}],\"Day\":{\"Icon\":1,\"IconPhrase\":\"Sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Sunny\",\"LongPhrase\":\"Sunny\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":321,\"Localized\":\"NW\",\"English\":\"NW\"}},\"WindGust\":{\"Speed\":{\"Value\":21.9,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":321,\"Localized\":\"NW\",\"English\":\"NW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":9,\"Evapotranspiration\":{\"Value\":0.23,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":8666.2,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":22,\"Maximum\":74,\"Average\":36},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":55,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":60,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":61,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":72,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":67,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":35,\"IconPhrase\":\"Partly cloudy\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Partly cloudy\",\"LongPhrase\":\"Partly cloudy\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":5.8,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":313,\"Localized\":\"NW\",\"English\":\"NW\"}},\"WindGust\":{\"Speed\":{\"Value\":18.4,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":318,\"Localized\":\"NW\",\"English\":\"NW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":30,\"Evapotranspiration\":{\"Value\":0.02,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":199.8,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":35,\"Maximum\":71,\"Average\":58},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":55,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":63,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/us/san-jose-ca/95110/daily-weather-forecast/347630?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/san-jose-ca/95110/daily-weather-forecast/347630?day=1&lang=en-us\"}]}",
	"la":     "{\"Headline\":{\"EffectiveDate\":\"2025-05-10T08:00:00-07:00\",\"EffectiveEpochDate\":1746889200,\"Severity\":4,\"Text\":\"Record-breaking high temperatures today\",\"Category\":\"record heat\",\"EndDate\":\"2025-05-10T20:00:00-07:00\",\"EndEpochDate\":1746932400,\"MobileLink\":\"http://www.accuweather.com/en/us/los-angeles-ca/90012/daily-weather-forecast/347625?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/los-angeles-ca/90012/daily-weather-forecast/347625?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00-07:00\",\"EpochDate\":1746885600,\"Sun\":{\"Rise\":\"2025-05-10T05:55:00-07:00\",\"EpochRise\":1746881700,\"Set\":\"2025-05-10T19:44:00-07:00\",\"EpochSet\":1746931440},\"Moon\":{\"Rise\":\"2025-05-10T18:17:00-07:00\",\"EpochRise\":1746926220,\"Set\":\"2025-05-11T05:03:00-07:00\",\"EpochSet\":1746964980,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":100,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"},\"Maximum\":{\"Value\":106,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Hot\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"},\"Maximum\":{\"Value\":98,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Hot\"}},\"HoursOfSun\":12.5,\"DegreeDaySummary\":{\"Heating\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":18,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":73,\"Category\":\"Moderate\",\"CategoryValue\":2,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":2,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":3250,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":50,\"Category\":\"Moderate\",\"CategoryValue\":2},{\"Name\":\"UVIndex\",\"Value\":11,\"Category\":\"Extreme\",\"CategoryValue\":5}],\"Day\":{\"Icon\":1,\"IconPhrase\":\"Sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Sunny; record-breaking heat\",\"LongPhrase\":\"Sunny and hot with the temperature breaking the record of 95 set in 1934; caution advised if outside for extended periods of time\",\"PrecipitationProbability\":1,\"ThunderstormProbability\":0,\"RainProbability\":1,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":4.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":228,\"Localized\":\"SW\",\"English\":\"SW\"}},\"WindGust\":{\"Speed\":{\"Value\":18.4,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":246,\"Localized\":\"WSW\",\"English\":\"WSW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":4,\"Evapotranspiration\":{\"Value\":0.27,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":8761.5,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":18,\"Maximum\":52,\"Average\":26},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":71,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":68,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":74,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":86,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":82,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":35,\"IconPhrase\":\"Partly cloudy\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Partly cloudy\",\"LongPhrase\":\"Partly cloudy\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":4.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":160,\"Localized\":\"SSE\",\"English\":\"SSE\"}},\"WindGust\":{\"Speed\":{\"Value\":13.8,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":233,\"Localized\":\"SW\",\"English\":\"SW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":45,\"Evapotranspiration\":{\"Value\":0.03,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":174.5,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":30,\"Maximum\":66,\"Average\":52},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":59,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":62,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":65,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":79,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":70,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/us/los-angeles-ca/90012/daily-weather-forecast/347625?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/los-angeles-ca/90012/daily-weather-forecast/347625?day=1&lang=en-us\"}]}",
	"nyc":    "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T08:00:00-04:00\",\"EffectiveEpochDate\":1746964800,\"Severity\":4,\"Text\":\"Pleasant tomorrow\",\"Category\":\"mild\",\"EndDate\":null,\"EndEpochDate\":null,\"MobileLink\":\"http://www.accuweather.com/en/us/new-york-ny/10021/daily-weather-forecast/14-349727_1_al?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/new-york-ny/10021/daily-weather-forecast/14-349727_1_al?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00-04:00\",\"EpochDate\":1746874800,\"Sun\":{\"Rise\":\"2025-05-10T05:44:00-04:00\",\"EpochRise\":1746870240,\"Set\":\"2025-05-10T20:01:00-04:00\",\"EpochSet\":1746921660},\"Moon\":{\"Rise\":\"2025-05-10T18:24:00-04:00\",\"EpochRise\":1746915840,\"Set\":\"2025-05-11T04:49:00-04:00\",\"EpochSet\":1746953340,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":71,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":74,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":67,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":9.8,\"DegreeDaySummary\":{\"Heating\":{\"Value\":1,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":54,\"Category\":\"Moderate\",\"CategoryValue\":2,\"Type\":\"Nitrogen Dioxide\"},{\"Name\":\"Grass\",\"Value\":2,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":5,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":50,\"Category\":\"Moderate\",\"CategoryValue\":2},{\"Name\":\"UVIndex\",\"Value\":9,\"Category\":\"Very High\",\"CategoryValue\":4}],\"Day\":{\"Icon\":3,\"IconPhrase\":\"Partly sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Partly sunny; breezy, warmer\",\"LongPhrase\":\"Partly sunny, breezy and warmer\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":10.4,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":300,\"Localized\":\"WNW\",\"English\":\"WNW\"}},\"WindGust\":{\"Speed\":{\"Value\":24.2,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":296,\"Localized\":\"WNW\",\"English\":\"WNW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":37,\"Evapotranspiration\":{\"Value\":0.15,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":7814.5,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":26,\"Maximum\":63,\"Average\":41},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":47,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":53,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":62,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":33,\"IconPhrase\":\"Clear\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Clear\",\"LongPhrase\":\"Clear\",\"PrecipitationProbability\":1,\"ThunderstormProbability\":0,\"RainProbability\":1,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":3.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":268,\"Localized\":\"W\",\"English\":\"W\"}},\"WindGust\":{\"Speed\":{\"Value\":11.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":281,\"Localized\":\"W\",\"English\":\"W\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":6,\"Evapotranspiration\":{\"Value\":0.02,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":240.1,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":34,\"Maximum\":63,\"Average\":48},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":52,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":56,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":61,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/us/new-york-ny/10021/daily-weather-forecast/14-349727_1_al?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/us/new-york-ny/10021/daily-weather-forecast/14-349727_1_al?day=1&lang=en-us\"}]}",
	"tokyo":  "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T19:00:00+09:00\",\"EffectiveEpochDate\":1746957600,\"Severity\":5,\"Text\":\"Rain Sunday night\",\"Category\":\"rain\",\"EndDate\":\"2025-05-12T07:00:00+09:00\",\"EndEpochDate\":1747000800,\"MobileLink\":\"http://www.accuweather.com/en/jp/tokyo/226396/daily-weather-forecast/226396?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/jp/tokyo/226396/daily-weather-forecast/226396?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-11T07:00:00+09:00\",\"EpochDate\":1746914400,\"Sun\":{\"Rise\":\"2025-05-11T04:40:00+09:00\",\"EpochRise\":1746906000,\"Set\":\"2025-05-11T18:35:00+09:00\",\"EpochSet\":1746956100},\"Moon\":{\"Rise\":\"2025-05-11T17:24:00+09:00\",\"EpochRise\":1746951840,\"Set\":\"2025-05-12T03:55:00+09:00\",\"EpochSet\":1746989700,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":62,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":81,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":82,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Warm\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":58,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cool\"},\"Maximum\":{\"Value\":77,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":1,\"DegreeDaySummary\":{\"Heating\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":6,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":5,\"Category\":\"Moderate\",\"CategoryValue\":2}],\"Day\":{\"Icon\":4,\"IconPhrase\":\"Intermittent clouds\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Breezy in the afternoon\",\"LongPhrase\":\"Sun through high clouds and less humid; breezy in the afternoon\",\"PrecipitationProbability\":9,\"ThunderstormProbability\":0,\"RainProbability\":9,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":10.4,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":261,\"Localized\":\"W\",\"English\":\"W\"}},\"WindGust\":{\"Speed\":{\"Value\":17.3,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":179,\"Localized\":\"S\",\"English\":\"S\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":99,\"Evapotranspiration\":{\"Value\":0.13,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":512,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":42,\"Maximum\":70,\"Average\":51},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":62,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":65,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":71,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":68,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":18,\"IconPhrase\":\"Rain\",\"HasPrecipitation\":true,\"PrecipitationType\":\"Rain\",\"PrecipitationIntensity\":\"Light\",\"ShortPhrase\":\"On-and-off rain and drizzle\",\"LongPhrase\":\"On-and-off rain and drizzle\",\"PrecipitationProbability\":84,\"ThunderstormProbability\":0,\"RainProbability\":84,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":9.2,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":233,\"Localized\":\"SW\",\"English\":\"SW\"}},\"WindGust\":{\"Speed\":{\"Value\":13.8,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":334,\"Localized\":\"NNW\",\"English\":\"NNW\"}},\"TotalLiquid\":{\"Value\":0.09,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0.09,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":1.5,\"HoursOfRain\":1.5,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":100,\"Evapotranspiration\":{\"Value\":0.03,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":9.8,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":58,\"Maximum\":85,\"Average\":73},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":59,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":63,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":61,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":60,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":67,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":63,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/jp/tokyo/226396/daily-weather-forecast/226396?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/jp/tokyo/226396/daily-weather-forecast/226396?day=1&lang=en-us\"}]}",
	"warsaw": "{\"Headline\":{\"EffectiveDate\":\"2025-05-10T23:00:00+02:00\",\"EffectiveEpochDate\":1746910800,\"Severity\":5,\"Text\":\"Expect showers late Saturday evening\",\"Category\":\"rain\",\"EndDate\":\"2025-05-11T05:00:00+02:00\",\"EndEpochDate\":1746932400,\"MobileLink\":\"http://www.accuweather.com/en/pl/warsaw/274663/daily-weather-forecast/274663?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/pl/warsaw/274663/daily-weather-forecast/274663?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00+02:00\",\"EpochDate\":1746853200,\"Sun\":{\"Rise\":\"2025-05-10T04:50:00+02:00\",\"EpochRise\":1746845400,\"Set\":\"2025-05-10T20:16:00+02:00\",\"EpochSet\":1746900960},\"Moon\":{\"Rise\":\"2025-05-10T18:14:00+02:00\",\"EpochRise\":1746893640,\"Set\":\"2025-05-11T03:54:00+02:00\",\"EpochSet\":1746928440,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":36,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":34,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cold\"},\"Maximum\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":34,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cold\"},\"Maximum\":{\"Value\":45,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"}},\"HoursOfSun\":4.8,\"DegreeDaySummary\":{\"Heating\":{\"Value\":22,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":300,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":2,\"Category\":\"Low\",\"CategoryValue\":1}],\"Day\":{\"Icon\":12,\"IconPhrase\":\"Showers\",\"HasPrecipitation\":true,\"PrecipitationType\":\"Rain\",\"PrecipitationIntensity\":\"Light\",\"ShortPhrase\":\"Cold; spotty morning showers\",\"LongPhrase\":\"A few morning showers; otherwise, low clouds and cold\",\"PrecipitationProbability\":80,\"ThunderstormProbability\":16,\"RainProbability\":80,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":11.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":344,\"Localized\":\"NNW\",\"English\":\"NNW\"}},\"WindGust\":{\"Speed\":{\"Value\":27.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":350,\"Localized\":\"N\",\"English\":\"N\"}},\"TotalLiquid\":{\"Value\":0.05,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0.05,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":1.5,\"HoursOfRain\":1.5,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":88,\"Evapotranspiration\":{\"Value\":0.06,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":2939.2,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":59,\"Maximum\":94,\"Average\":74},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":41,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":44,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":47,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":49,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":39,\"IconPhrase\":\"Partly cloudy w/ showers\",\"HasPrecipitation\":true,\"PrecipitationType\":\"Rain\",\"PrecipitationIntensity\":\"Light\",\"ShortPhrase\":\"A shower or two this evening\",\"LongPhrase\":\"A couple of brief showers late this evening; clearing and chilly\",\"PrecipitationProbability\":49,\"ThunderstormProbability\":10,\"RainProbability\":49,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":5.8,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":355,\"Localized\":\"N\",\"English\":\"N\"}},\"WindGust\":{\"Speed\":{\"Value\":16.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":10,\"Localized\":\"N\",\"English\":\"N\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":1,\"HoursOfRain\":1,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":42,\"Evapotranspiration\":{\"Value\":0.01,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":398.1,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":59,\"Maximum\":95,\"Average\":75},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":36,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":42,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":40,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":36,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":43,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/pl/warsaw/274663/daily-weather-forecast/274663?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/pl/warsaw/274663/daily-weather-forecast/274663?day=1&lang=en-us\"}]}",
	"zurich": "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T08:00:00+02:00\",\"EffectiveEpochDate\":1746943200,\"Severity\":4,\"Text\":\"Pleasant Sunday\",\"Category\":\"mild\",\"EndDate\":null,\"EndEpochDate\":null,\"MobileLink\":\"http://www.accuweather.com/en/ch/zurich/316622/daily-weather-forecast/316622?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/ch/zurich/316622/daily-weather-forecast/316622?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00+02:00\",\"EpochDate\":1746853200,\"Sun\":{\"Rise\":\"2025-05-10T05:56:00+02:00\",\"EpochRise\":1746849360,\"Set\":\"2025-05-10T20:49:00+02:00\",\"EpochSet\":1746902940},\"Moon\":{\"Rise\":\"2025-05-10T18:54:00+02:00\",\"EpochRise\":1746896040,\"Set\":\"2025-05-11T04:58:00+02:00\",\"EpochSet\":1746932280,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":43,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":68,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":75,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":66,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":12.3,\"DegreeDaySummary\":{\"Heating\":{\"Value\":10,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":517,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":8,\"Category\":\"Very High\",\"CategoryValue\":4}],\"Day\":{\"Icon\":2,\"IconPhrase\":\"Mostly sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Mostly sunny and milder\",\"LongPhrase\":\"Mostly sunny and milder\",\"PrecipitationProbability\":4,\"ThunderstormProbability\":0,\"RainProbability\":4,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":3.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":62,\"Localized\":\"ENE\",\"English\":\"ENE\"}},\"WindGust\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":70,\"Localized\":\"ENE\",\"English\":\"ENE\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":21,\"Evapotranspiration\":{\"Value\":0.15,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":8153.8,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":42,\"Maximum\":70,\"Average\":55},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":44,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":52,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":48,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":59,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":33,\"IconPhrase\":\"Clear\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Clear and chilly\",\"LongPhrase\":\"Clear and chilly\",\"PrecipitationProbability\":3,\"ThunderstormProbability\":0,\"RainProbability\":3,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":3.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":43,\"Localized\":\"NE\",\"English\":\"NE\"}},\"WindGust\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":43,\"Localized\":\"NE\",\"English\":\"NE\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":6,\"Evapotranspiration\":{\"Value\":0.01,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":364.9,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":41,\"Maximum\":94,\"Average\":72},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":42,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":51,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":43,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":60,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/ch/zurich/316622/daily-weather-forecast/316622?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/ch/zurich/316622/daily-weather-forecast/316622?day=1&lang=en-us\"}]}",
	"berlin": "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T20:00:00+02:00\",\"EffectiveEpochDate\":1746986400,\"Severity\":7,\"Text\":\"Cool Sunday night\",\"Category\":\"cold\",\"EndDate\":\"2025-05-12T08:00:00+02:00\",\"EndEpochDate\":1747029600,\"MobileLink\":\"http://www.accuweather.com/en/de/berlin/10178/daily-weather-forecast/178087?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/de/berlin/10178/daily-weather-forecast/178087?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00+02:00\",\"EpochDate\":1746853200,\"Sun\":{\"Rise\":\"2025-05-10T05:19:00+02:00\",\"EpochRise\":1746847140,\"Set\":\"2025-05-10T20:47:00+02:00\",\"EpochSet\":1746902820},\"Moon\":{\"Rise\":\"2025-05-10T18:46:00+02:00\",\"EpochRise\":1746895560,\"Set\":\"2025-05-11T04:23:00+02:00\",\"EpochSet\":1746930180,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":44,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":68,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":41,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Chilly\"},\"Maximum\":{\"Value\":69,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":41,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Cold\"},\"Maximum\":{\"Value\":65,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Pleasant\"}},\"HoursOfSun\":6.4,\"DegreeDaySummary\":{\"Heating\":{\"Value\":9,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":330,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":3,\"Category\":\"Moderate\",\"CategoryValue\":2}],\"Day\":{\"Icon\":4,\"IconPhrase\":\"Intermittent clouds\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Times of clouds and sun\",\"LongPhrase\":\"Times of clouds and sun\",\"PrecipitationProbability\":25,\"ThunderstormProbability\":0,\"RainProbability\":25,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":359,\"Localized\":\"N\",\"English\":\"N\"}},\"WindGust\":{\"Speed\":{\"Value\":23,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":79,\"Localized\":\"E\",\"English\":\"E\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":72,\"Evapotranspiration\":{\"Value\":0.1,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":4596.7,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":49,\"Maximum\":75,\"Average\":61},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":57,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":54,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":54,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":64,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":59,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":34,\"IconPhrase\":\"Mostly clear\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Mainly clear\",\"LongPhrase\":\"Mainly clear\",\"PrecipitationProbability\":4,\"ThunderstormProbability\":0,\"RainProbability\":4,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":65,\"Localized\":\"ENE\",\"English\":\"ENE\"}},\"WindGust\":{\"Speed\":{\"Value\":19.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":90,\"Localized\":\"E\",\"English\":\"E\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":22,\"Evapotranspiration\":{\"Value\":0.02,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":453.7,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":60,\"Maximum\":75,\"Average\":67},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":41,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":54,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":46,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":44,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":59,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":50,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/de/berlin/10178/daily-weather-forecast/178087?day=1&lang=en-us\",\"Link\":\"http://www.accuweather.com/en/de/berlin/10178/daily-weather-forecast/178087?day=1&lang=en-us\"}]}",
	"dubai":  "{\"Headline\":{\"EffectiveDate\":\"2025-05-11T01:00:00+04:00\",\"EffectiveEpochDate\":1746910800,\"Severity\":7,\"Text\":\"Warm late Saturday night\",\"Category\":\"heat\",\"EndDate\":\"2025-05-11T07:00:00+04:00\",\"EndEpochDate\":1746932400,\"MobileLink\":\"http://www.accuweather.com/en/ae/dubai/323091/daily-weather-forecast/323091?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/ae/dubai/323091/daily-weather-forecast/323091?lang=en-us\"},\"DailyForecasts\":[{\"Date\":\"2025-05-10T07:00:00+04:00\",\"EpochDate\":1746846000,\"Sun\":{\"Rise\":\"2025-05-10T05:37:00+04:00\",\"EpochRise\":1746841020,\"Set\":\"2025-05-10T18:54:00+04:00\",\"EpochSet\":1746888840},\"Moon\":{\"Rise\":\"2025-05-10T17:04:00+04:00\",\"EpochRise\":1746882240,\"Set\":\"2025-05-11T04:28:00+04:00\",\"EpochSet\":1746923280,\"Phase\":\"WaxingGibbous\",\"Age\":13},\"Temperature\":{\"Minimum\":{\"Value\":81,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":100,\"Unit\":\"F\",\"UnitType\":18}},\"RealFeelTemperature\":{\"Minimum\":{\"Value\":88,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Warm\"},\"Maximum\":{\"Value\":108,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Dangerous Heat\"}},\"RealFeelTemperatureShade\":{\"Minimum\":{\"Value\":88,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Warm\"},\"Maximum\":{\"Value\":103,\"Unit\":\"F\",\"UnitType\":18,\"Phrase\":\"Very Hot\"}},\"HoursOfSun\":11.5,\"DegreeDaySummary\":{\"Heating\":{\"Value\":0,\"Unit\":\"F\",\"UnitType\":18},\"Cooling\":{\"Value\":26,\"Unit\":\"F\",\"UnitType\":18}},\"AirAndPollen\":[{\"Name\":\"AirQuality\",\"Value\":0,\"Category\":\"Good\",\"CategoryValue\":1,\"Type\":\"Ozone\"},{\"Name\":\"Grass\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Mold\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Ragweed\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"Tree\",\"Value\":0,\"Category\":\"Low\",\"CategoryValue\":1},{\"Name\":\"UVIndex\",\"Value\":12,\"Category\":\"Extreme\",\"CategoryValue\":5}],\"Day\":{\"Icon\":2,\"IconPhrase\":\"Mostly sunny\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Mostly sunny and very warm\",\"LongPhrase\":\"Mostly sunny and very warm\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":8.1,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":29,\"Localized\":\"NNE\",\"English\":\"NNE\"}},\"WindGust\":{\"Speed\":{\"Value\":29.9,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":340,\"Localized\":\"NNW\",\"English\":\"NNW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":15,\"Evapotranspiration\":{\"Value\":0.26,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":8508.5,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":40,\"Maximum\":74,\"Average\":52},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":75,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":80,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":78,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":82,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":90,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":86,\"Unit\":\"F\",\"UnitType\":18}}},\"Night\":{\"Icon\":33,\"IconPhrase\":\"Clear\",\"HasPrecipitation\":false,\"ShortPhrase\":\"Clear and very warm\",\"LongPhrase\":\"Clear and very warm\",\"PrecipitationProbability\":0,\"ThunderstormProbability\":0,\"RainProbability\":0,\"SnowProbability\":0,\"IceProbability\":0,\"Wind\":{\"Speed\":{\"Value\":4.6,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":25,\"Localized\":\"NNE\",\"English\":\"NNE\"}},\"WindGust\":{\"Speed\":{\"Value\":11.5,\"Unit\":\"mi/h\",\"UnitType\":9},\"Direction\":{\"Degrees\":340,\"Localized\":\"NNW\",\"English\":\"NNW\"}},\"TotalLiquid\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Rain\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Snow\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"Ice\":{\"Value\":0,\"Unit\":\"in\",\"UnitType\":1},\"HoursOfPrecipitation\":0,\"HoursOfRain\":0,\"HoursOfSnow\":0,\"HoursOfIce\":0,\"CloudCover\":0,\"Evapotranspiration\":{\"Value\":0.03,\"Unit\":\"in\",\"UnitType\":1},\"SolarIrradiance\":{\"Value\":141.9,\"Unit\":\"W/m²\",\"UnitType\":33},\"RelativeHumidity\":{\"Minimum\":44,\"Maximum\":74,\"Average\":59},\"WetBulbTemperature\":{\"Minimum\":{\"Value\":73,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":78,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":76,\"Unit\":\"F\",\"UnitType\":18}},\"WetBulbGlobeTemperature\":{\"Minimum\":{\"Value\":81,\"Unit\":\"F\",\"UnitType\":18},\"Maximum\":{\"Value\":86,\"Unit\":\"F\",\"UnitType\":18},\"Average\":{\"Value\":84,\"Unit\":\"F\",\"UnitType\":18}}},\"Sources\":[\"AccuWeather\"],\"MobileLink\":\"http://www.accuweather.com/en/ae/dubai/323091/daily-weather-forecast/323091?lang=en-us\",\"Link\":\"http://www.accuweather.com/en/ae/dubai/323091/daily-weather-forecast/323091?lang=en-us\"}]}",
	
}

var supportedLocations = map[string]location{
	"london": {nil, 51.507351, -0.127758, "Europe/London", "London"},
	"sf":     {nil, 37.774929, -122.419418, "America/Los_Angeles", "San Francisco"},
	"sj":     {nil, 37.338207, -121.886330, "America/Los_Angeles", "San Jose"},
	"la":     {nil, 34.052235, -118.243683, "America/Los_Angeles", "Los Angeles"},
	"nyc":    {nil, 40.712776, -74.005974, "America/New_York", "New York City"},
	"tokyo":  {nil, 35.689487, 139.691711, "Asia/Tokyo", "Tokyo"},
	"manila": {nil, 14.599512, 120.984222, "Asia/Dubai", "Manila"},
	"warsaw": {nil, 52.229675, 21.012230, "Europe/Warsaw", "Warsaw"},
	"zurich": {nil, 47.369019, 8.538030, "Europe/Zurich", "Zurich"},
	"berlin": {nil, 52.520008, 13.404954, "Europe/Berlin", "Berlin"},
	"dubai":  {nil, 25.204849, 55.270782, "Asia/Dubai", "Dubai"},
	

}

func main() {
	port := flag.Int("port", 8080, "the port that the server should listen on")
	genKeys := flag.Bool("generate-vapid-keys", false, "generate a new vapid key pair, which will be outputted to stdout.")
	usePlaceholder := flag.Bool("use-placeholder", false, "use placeholder data instead of real API data.")

	flag.Parse()

	if *genKeys {
		generateKeys()
		return
	}

	slog.Info("starting 7am...")

	_ = godotenv.Load()
	err := checkEnv()
	if err != nil {
		log.Fatal(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	p := filepath.Join(wd, "data")
	err = os.MkdirAll(p, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	slog.Info("data directory created", "path", p)

	db, err := initDB()
	if err != nil {
		log.Fatalf("failed to initialize db: %e\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genaiClient, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  os.Getenv("GEMINI_API_KEY"),
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		log.Fatalf("failed to initialize gemini client: %e\n", err)
	}

	summaryHTML, _ := webDir.ReadFile("web/summary.html")
	summaryPageTemplate, _ := template.New("summary.html").Parse(string(summaryHTML))

	state := state{
		ctx:             ctx,
		db:              db,
		metAPIUserAgent: os.Getenv("MET_API_USER_AGENT"),
		template: pageTemplate{
			summary: summaryPageTemplate,
		},
		summaries:    sync.Map{},
		summaryChans: map[string]chan string{},
		genai:        genaiClient,

		usePlaceholder: *usePlaceholder,

		subscriptions: map[string][]*registeredSubscription{},

		vapidSubject:    os.Getenv("VAPID_SUBJECT"),
		vapidPublicKey:  os.Getenv("VAPID_PUBLIC_KEY_BASE64"),
		vapidPrivateKey: os.Getenv("VAPID_PRIVATE_KEY_BASE64"),
	}

	var schedulers []gocron.Scheduler

	// schedule periodic updates of weather summary for each supported location
	for locKey, loc := range supportedLocations {
		l, err := time.LoadLocation(loc.ianaName)
		if err != nil {
			log.Fatal(err)
		}

		loc.tz = l

		s, err := gocron.NewScheduler(gocron.WithLocation(l))
		if err != nil {
			log.Fatal(err)
		}

		_, err = s.NewJob(
			gocron.DailyJob(1, gocron.NewAtTimes(gocron.NewAtTime(7, 0, 0))),
			gocron.NewTask(updateSummary, &state, locKey, &loc),
			gocron.WithStartAt(gocron.WithStartImmediately()),
		)
		if err != nil {
			log.Fatal(err)
		}

		schedulers = append(schedulers, s)
		c := make(chan string)

		state.subscriptions[locKey] = []*registeredSubscription{}
		state.summaryChans[locKey] = c

		// listen for summary updates, and publish updates to all update subscribers via web push
		go listenForSummaryUpdates(&state, locKey)

		s.Start()

		slog.Info("update job scheduled", "location", locKey)
	}

	err = loadSubscriptions(&state)
	if err != nil {
		log.Fatalf("failed to load existing subscriptions: %e\n", err)
	}

	http.HandleFunc("/", handleHTTPRequest(&state))

	slog.Info("server starting", "port", *port)

	err = http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if err != nil {
		log.Printf("failed to start http server: %e\n", err)
	}

	for _, s := range schedulers {
		s.Shutdown()
	}

	slog.Info("7am shut down")
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
		CREATE TABLE IF NOT EXISTS subscriptions(
			id TEXT PRIMARY KEY,
			locations TEXT NOT NULL,
			subscription_json TEXT NOT NULL
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

func updateSummary(state *state, locKey string, loc *location) {
	slog.Info("updating weather summary", "location", locKey)

	var weatherJSON string
	if state.usePlaceholder {
		weatherJSON = placeholderWeather[locKey]
	} else {
		req, err := http.NewRequest("GET", fmt.Sprintf("https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=%v&lon=%v", loc.lat, loc.lon), nil)
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

		b, err := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		if err != nil {
			slog.Error("failed to query weather data", "location", locKey, "error", err)
			return
		}

		weatherJSON = string(b)
	}

	date := time.Now().In(loc.tz).Format("2006-02-01")

	result, err := state.genai.Models.GenerateContent(state.ctx, "gemini-2.0-flash", []*genai.Content{{
		Parts: []*genai.Part{
			{Text: fmt.Sprintf(prompt, date, loc.displayName, loc.displayName)},
			{Text: weatherJSON},
		},
	}}, nil)
	if err != nil {
		slog.Error("failed to generate weather summary", "location", locKey, "error", err)
		return
	}

	summary := result.Text()
	c := state.summaryChans[locKey]

	state.summaries.Store(locKey, summary)
	if len(state.subscriptions[locKey]) > 0 {
		c <- summary
	}

	slog.Info("updated weather summary", "location", locKey)
}

func listenForSummaryUpdates(state *state, locKey string) {
	c := state.summaryChans[locKey]

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
