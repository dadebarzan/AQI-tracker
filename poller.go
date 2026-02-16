package main

import (
	"context"
	"encoding/csv"
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
	kafkago "github.com/segmentio/kafka-go"
)

var apiKey string
var apiHost string
var kafkaBroker string
var kafkaWriter *kafkago.Writer
var httpClient *http.Client
var cities []City
var numWorkers int
var pollInterval time.Duration

const (
	maxStartupJitter = 1000 * time.Millisecond
)

var expectedCSVHeader = []string{"name", "country", "continent"}

func init() {
	// Load .env file
	_ = godotenv.Load()

	// Load environment variables
	apiKey = os.Getenv("AQI_API_KEY")
	apiHost = os.Getenv("AQI_API_HOST")
	kafkaBroker = os.Getenv("KAFKA_BROKER")

	numWorkersStr := os.Getenv("NUM_WORKERS")
	pollIntervalStr := os.Getenv("POLL_INTERVAL")

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
}

// --- Data Structures (JSON Mapping) ---
type City struct {
	Name      string
	Country   string
	Continent string
}

type AQIResponse struct {
	Status string  `json:"status"`
	Data   AQIData `json:"data"`
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

func loadCitiesFromCSV(filename string) ([]City, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open cities CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	// Validate and skip header
	if err := validateCSVHeader(reader); err != nil {
		return nil, err
	}

	// Read city records
	cities, err := readCityRecords(reader)
	if err != nil {
		return nil, err
	}

	if len(cities) == 0 {
		return nil, fmt.Errorf("no valid cities found in CSV")
	}

	return cities, nil
}

func validateCSVHeader(reader *csv.Reader) error {
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	if len(header) != len(expectedCSVHeader) {
		return fmt.Errorf("invalid CSV header length: expected %d columns, got %d", len(expectedCSVHeader), len(header))
	}

	for i, h := range header {
		if h != expectedCSVHeader[i] {
			return fmt.Errorf("invalid CSV header at column %d: expected '%s', got '%s'", i+1, expectedCSVHeader[i], h)
		}
	}

	return nil
}

func readCityRecords(reader *csv.Reader) ([]City, error) {
	var cities []City
	lineNum := 2 // First data line after header

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV at line %d: %w", lineNum, err)
		}

		// Validate record length
		if len(record) != len(expectedCSVHeader) {
			log.Printf("Warning: skipping malformed line %d: expected %d fields, got %d", lineNum, len(expectedCSVHeader), len(record))
			lineNum++
			continue
		}

		// Skip empty lines
		if record[0] == "" {
			lineNum++
			continue
		}

		// Check for Country and Continent presence (informational only; city is still added if missing)
		if record[1] == "" || record[2] == "" {
			log.Printf("Info: line %d is missing country or continent (optional fields, city will still be added): %v", lineNum, record)
		}

		city := City{
			Name:      record[0],
			Country:   record[1],
			Continent: record[2],
		}
		cities = append(cities, city)
		lineNum++
	}

	return cities, nil
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

func transformToEvent(city string, aqiResp AQIResponse) AQIEvent {
	return AQIEvent{
		City:        city,
		AQI:         aqiResp.Data.AQI,
		CO:          aqiResp.Data.IAQI.CO.V,
		NO2:         aqiResp.Data.IAQI.NO2.V,
		PM10:        aqiResp.Data.IAQI.PM10.V,
		PM25:        aqiResp.Data.IAQI.PM25.V,
		Temperature: aqiResp.Data.IAQI.Temp.V,
		Timestamp:   aqiResp.Data.Time.V,
		Latitude:    aqiResp.Data.City.Geo[0],
		Longitude:   aqiResp.Data.City.Geo[1],
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

	if aqiResp.Status != "ok" {
		log.Printf("[%s] API returned non-ok status: %s", city, aqiResp.Status)
		return
	}

	// Transform to event
	event := transformToEvent(city, aqiResp)

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
func scheduler(tasks chan<- string, cities []City, interval time.Duration, stopCh <-chan struct{}) {
	defer close(tasks) // Close channel when scheduler exits

	if len(cities) == 0 {
		log.Println("No cities configured; scheduler will not start polling")
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial poll with jitter
	log.Println("Starting initial poll with jitter...")
	for _, city := range cities {
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

	// Periodic polling
	for {
		select {
		case <-ticker.C:
			log.Printf("Scheduling poll for %d cities", len(cities))
			for _, city := range cities {
				select {
				case tasks <- city.Name:
				case <-stopCh:
					log.Println("Scheduler stopped during periodic poll")
					return
				}
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
	cities, err = loadCitiesFromCSV("cities.csv")
	if err != nil {
		log.Printf("Warning: failed to load cities from CSV, starting with no cities: %v", err)
	} else {
		log.Printf("Loaded %d cities from CSV", len(cities))
	}

	defer func() {
		log.Println("Shutting down poller...")
		if err := kafkaWriter.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
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
	go scheduler(tasks, cities, pollInterval, stopCh)

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
