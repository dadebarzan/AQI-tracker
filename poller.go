package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	kafkago "github.com/segmentio/kafka-go"
)

var apiKey string
var apiHost string
var kafkaBroker string
var kafkaWriter *kafkago.Writer
var httpClient *http.Client
var db *sql.DB
var cities []City
var numWorkers int
var pollInterval time.Duration
var reloadInterval time.Duration

const (
	maxStartupJitter = 1000 * time.Millisecond
)

func init() {
	// Load .env file
	_ = godotenv.Load()

	// Load environment variables
	apiKey = os.Getenv("AQI_API_KEY")
	apiHost = os.Getenv("AQI_API_HOST")
	kafkaBroker = os.Getenv("KAFKA_BROKER")

	numWorkersStr := os.Getenv("NUM_WORKERS")
	pollIntervalStr := os.Getenv("POLL_INTERVAL")
	reloadIntervalStr := os.Getenv("RELOAD_INTERVAL")
	if numWorkersStr == "" {
		numWorkers = 50 // Default
	} else {
		var err error
		numWorkers, err = strconv.Atoi(numWorkersStr)
		if err != nil {
			log.Fatalf("Invalid NUM_WORKERS: %v", err)
		}

		if numWorkers <= 0 {
			numWorkers = 50 // Fallback to default if invalid
		}
	}

	if pollIntervalStr == "" {
		pollInterval = 10 * time.Minute // Default
	} else {
		pollIntervalInt, err := strconv.Atoi(pollIntervalStr)
		if err != nil {
			log.Fatalf("Invalid POLL_INTERVAL: %v", err)
		}

		pollInterval = time.Duration(pollIntervalInt) * time.Minute
		if pollInterval <= 0 {
			pollInterval = 10 * time.Minute // Fallback to default if invalid
		}
	}

	if reloadIntervalStr == "" {
		reloadInterval = 60 * time.Minute // Default
	} else {
		reloadIntervalInt, err := strconv.Atoi(reloadIntervalStr)
		if err != nil {
			log.Fatalf("Invalid RELOAD_INTERVAL: %v", err)
		}

		reloadInterval = time.Duration(reloadIntervalInt) * time.Minute
		if reloadInterval <= 0 {
			reloadInterval = 60 * time.Minute // Fallback to default if invalid
		}
	}

	if apiKey == "" || apiHost == "" {
		log.Fatal("Missing required environment variables: AQI_API_KEY and AQI_API_HOST")
	}

	if kafkaBroker == "" {
		kafkaBroker = "kafka:29092" // Default
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialize shared HTTP client with connection pooling
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}

	// Initialize Kafka writer
	kafkaWriter = &kafkago.Writer{
		Addr:         kafkago.TCP(kafkaBroker),
		Topic:        "aqi-raw",
		Balancer:     &kafkago.Hash{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafkago.RequireOne,
		Compression:  kafkago.Snappy,
		MaxAttempts:  3,
	}

	log.Printf("Initialized HTTP client and Kafka writer (broker: %s, topic: aqi-raw)", kafkaBroker)

	// Initialize database connection
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "postgres" // Default
	}
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbUser, dbPass, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Wait for database to be ready
	maxAttempts := 10
	for i := 1; i <= maxAttempts; i++ {
		err = db.Ping()
		if err == nil {
			log.Println("Successfully connected to the database")
			break
		}
		if i == maxAttempts {
			log.Fatalf("Database connection failed after %d attempts: %v", maxAttempts, err)
		}
		log.Printf("Database not ready (attempt %d/%d failed: %v)", i, maxAttempts, err)
		time.Sleep(2 * time.Second)
	}
}

// --- Data Structures ---
type City struct {
	ID         int
	Name       string
	Country    string
	Continent  string
	ValidEntry bool
	CreatedAt  time.Time
}

type AQIResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

type AQIData struct {
	AQI         int      `json:"aqi"`
	Idx         int      `json:"idx"`
	City        CityInfo `json:"city"`
	Dominentpol string   `json:"dominentpol"`
	IAQI        IQAI     `json:"iaqi"`
	Time        TimeInfo `json:"time"`
}

type CityInfo struct {
	Geo      [2]float64 `json:"geo"`
	Name     string     `json:"name"`
	URL      string     `json:"url"`
	Location string     `json:"location"`
}

type IQAI struct {
	CO   Pollutant `json:"co"`
	NO2  Pollutant `json:"no2"`
	PM10 Pollutant `json:"pm10"`
	PM25 Pollutant `json:"pm25"`
	Temp Pollutant `json:"t"`
}

type Pollutant struct {
	V float64 `json:"v"`
}

type TimeInfo struct {
	S   string `json:"s"`
	Tz  string `json:"tz"`
	V   int64  `json:"v"`
	ISO string `json:"iso"`
}

// AQI event to send to Kafka
type AQIEvent struct {
	City        string  `json:"city"`
	AQI         int     `json:"aqi"`
	CO          float64 `json:"co"`
	NO2         float64 `json:"no2"`
	PM10        float64 `json:"pm10"`
	PM25        float64 `json:"pm25"`
	Temperature float64 `json:"temperature"`
	Timestamp   int64   `json:"timestamp"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
}

func loadCitiesFromDB() ([]City, error) {
	query := `
        SELECT id, name, country, continent, valid_entry, created_at
		FROM cities
		WHERE valid_entry = true
		ORDER BY name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query cities: %w", err)
	}
	defer rows.Close()

	var cities []City
	for rows.Next() {
		var city City

		err := rows.Scan(
			&city.ID,
			&city.Name,
			&city.Country,
			&city.Continent,
			&city.ValidEntry,
			&city.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan city row: %w", err)
		}
		cities = append(cities, city)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating city rows: %w", err)
	}

	return cities, nil
}

func markCityAsInvalid(cityName string) error {
	query := `
        UPDATE cities 
        SET valid_entry = false 
        WHERE name = $1
    `

	result, err := db.Exec(query, cityName)
	if err != nil {
		return fmt.Errorf("failed to update city: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("city not found in database")
	}

	log.Printf("[%s] ⚠️  Marked as invalid (Unknown station)", cityName)
	return nil
}

func fetchData(requestURL string) ([]byte, error) {
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer res.Body.Close()

	// Check status code before reading body
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("non-2xx response: %d %s - %s", res.StatusCode, res.Status, string(body))
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

func sendToKafka(ctx context.Context, event AQIEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = kafkaWriter.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(event.City),
		Value: eventJSON,
		Time:  time.Now(),
	},
	)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func transformToEvent(city string, aqiData AQIData) AQIEvent {
	return AQIEvent{
		City:        city,
		AQI:         aqiData.AQI,
		CO:          aqiData.IAQI.CO.V,
		NO2:         aqiData.IAQI.NO2.V,
		PM10:        aqiData.IAQI.PM10.V,
		PM25:        aqiData.IAQI.PM25.V,
		Temperature: aqiData.IAQI.Temp.V,
		Timestamp:   aqiData.Time.V,
		Latitude:    aqiData.City.Geo[0],
		Longitude:   aqiData.City.Geo[1],
	}
}

func buildAQIURL(city string) (string, error) {
	// Validate city name (basic check)
	if city == "" {
		return "", fmt.Errorf("city name cannot be empty")
	}

	escapedCity := url.PathEscape(city)

	// Build the full URL
	u := &url.URL{
		Scheme: "https",
		Host:   apiHost,
		Path:   fmt.Sprintf("/feed/%s/", escapedCity),
	}

	q := u.Query()
	q.Set("token", apiKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func pollCity(city string) {
	log.Printf("Checking AQI for city: %s", city)

	apiURL, err := buildAQIURL(city)
	if err != nil {
		log.Printf("[%s] Error building URL: %v", city, err)
		return
	}

	body, err := fetchData(apiURL)
	if err != nil {
		log.Printf("[%s] Error fetching AQI: %v", city, err)
		return
	}

	// Parse JSON response
	var aqiResp AQIResponse
	if err := json.Unmarshal(body, &aqiResp); err != nil {
		log.Printf("[%s] Error parsing AQI JSON: %v", city, err)
		return
	}

	if aqiResp.Status == "error" {
		var errorMsg string
		if err := json.Unmarshal(aqiResp.Data, &errorMsg); err == nil {
			if errorMsg == "Unknown station" {
				// Mark city as invalid in database
				if err := markCityAsInvalid(city); err != nil {
					log.Printf("[%s] Error marking city as invalid: %v", city, err)
				}
				return
			}
			log.Printf("[%s] API error: %s", city, errorMsg)
			return
		}
		log.Printf("[%s] API returned error status without message", city)
		return
	}

	if aqiResp.Status != "ok" {
		log.Printf("[%s] API returned non-ok status: %s", city, aqiResp.Status)
		return
	}

	// Check if data is a string (error) or object
	var dataStr string
	if err := json.Unmarshal(aqiResp.Data, &dataStr); err == nil {
		log.Printf("[%s] API error: %s", city, dataStr)
		return
	}

	var aqiData AQIData
	if err := json.Unmarshal(aqiResp.Data, &aqiData); err != nil {
		log.Printf("[%s] Error parsing AQI data: %v", city, err)
		return
	}

	// Transform to event
	event := transformToEvent(city, aqiData)

	// Send to Kafka with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sendToKafka(ctx, event); err != nil {
		log.Printf("[%s] Error sending to Kafka: %v", city, err)
	} else {
		log.Printf("[%s] ✅ Sent to Kafka - AQI: %d", city, event.AQI)
	}
}

// Worker function - processes tasks from the channel
func worker(id int, tasks <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range tasks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Worker %d] Panic recovered: %v", id, r)
				}
			}()
			pollCity(task)
		}()
	}

	log.Printf("[Worker %d] Shutting down", id)
}

// Scheduler - sends poll tasks to workers at regular intervals
func scheduler(tasks chan<- string, citiesPtr *[]City, citiesMutex *sync.RWMutex, pollInterval, reloadInterval time.Duration, stopCh <-chan struct{}) {
	defer close(tasks) // Close channel when scheduler exits

	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	reloadTicker := time.NewTicker(reloadInterval)
	defer reloadTicker.Stop()

	// Helper function to get current cities safely
	getCities := func() []City {
		citiesMutex.RLock()
		defer citiesMutex.RUnlock()
		citiesCopy := make([]City, len(*citiesPtr))
		copy(citiesCopy, *citiesPtr)
		return citiesCopy
	}

	currentCities := getCities()
	if len(currentCities) == 0 {
		log.Println("No cities configured yet; waiting for database to populate or reload...")
	} else {
		// Initial poll with jitter
		log.Println("Starting initial poll with jitter...")
		for _, city := range currentCities {
			go func(c City) {
				jitter := time.Duration(rand.Int63n(int64(maxStartupJitter)))
				timer := time.NewTimer(jitter)
				defer timer.Stop()

				select {
				case <-timer.C:
					select {
					case tasks <- c.Name:
					case <-stopCh:
						log.Println("Scheduler stopped during initial poll")
						return
					}
				case <-stopCh:
					log.Println("Scheduler stopped during initial poll")
					return
				}
			}(city)
		}
		log.Println("Initial poll completed")
	}

	// Periodic polling
	for {
		select {
		case <-pollTicker.C:
			currentCities := getCities()
			if len(currentCities) == 0 {
				log.Println("No cities to poll (skipping poll cycle)")
				continue
			}

			log.Printf("Scheduling poll for %d cities", len(currentCities))
			for _, city := range currentCities {
				select {
				case tasks <- city.Name:
				case <-stopCh:
					log.Println("Scheduler stopped during periodic poll")
					return
				}
			}

		case <-reloadTicker.C:
			log.Println("Reloading cities from database...")
			newCities, err := loadCitiesFromDB()
			if err != nil {
				log.Printf("Failed to reload cities: %v", err)
				continue
			}

			citiesMutex.Lock()
			oldCount := len(*citiesPtr)
			*citiesPtr = newCities
			newCount := len(*citiesPtr)
			citiesMutex.Unlock()

			if oldCount == 0 && newCount > 0 {
				log.Printf("Cities loaded: 0 → %d (scheduler now active!)", newCount)
			} else if newCount != oldCount {
				log.Printf("Cities reloaded: %d → %d (change: %+d)", oldCount, newCount, newCount-oldCount)
			} else {
				log.Printf("Cities reloaded: %d (no changes)", newCount)
			}

		case <-stopCh:
			log.Println("Scheduler received stop signal")
			return
		}
	}
}

// --- MAIN ---
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("Starting AQI Poller service")

	// Load cities from CSV
	var err error
	cities, err = loadCitiesFromDB()
	if err != nil {
		log.Printf("Warning: failed to load cities from database, starting with no cities: %v", err)
		cities = []City{}
	} else {
		log.Printf("Loaded %d cities from database", len(cities))
	}

	var citiesMutex sync.RWMutex

	defer func() {
		log.Println("Shutting down poller...")
		if err := kafkaWriter.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
		}
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
		httpClient.CloseIdleConnections()
		log.Printf("Shutdown complete")
	}()

	log.Printf("Monitoring %d cities", len(cities))

	// Create task channel (buffered to avoid blocking scheduler)
	tasks := make(chan string, len(cities))

	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, tasks, &wg)
		log.Printf("Started worker %d", i)
	}

	// Start scheduler
	go scheduler(tasks, &cities, &citiesMutex, pollInterval, reloadInterval, stopCh)

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Signal scheduler to stop
	close(stopCh)

	// Wait for all workers to complete
	log.Println("Waiting for workers to finish...")
	wg.Wait()

	log.Println("All workers stopped, exiting")
}
