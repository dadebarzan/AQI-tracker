package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

const (
	maxStartupJitter = 1000 * time.Millisecond
)

var cities []City
var numWorkers int
var pollInterval time.Duration
var reloadInterval time.Duration
var invalidateCity = markCityAsInvalid

// Helper function
func getEnvAsInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Fatalf("Invalid %s: %v", key, err)
	}

	if val <= 0 {
		return defaultVal // Fallback to default if invalid
	}

	return val
}

func loadConfig() {
	numWorkers = getEnvAsInt("NUM_WORKERS", 50)
	pollInterval = time.Duration(getEnvAsInt("POLL_INTERVAL", 30)) * time.Minute
	reloadInterval = time.Duration(getEnvAsInt("RELOAD_INTERVAL", 60)) * time.Minute

	log.Printf("Configuration - NUM_WORKERS: %d, POLL_INTERVAL: %s, RELOAD_INTERVAL: %s",
		numWorkers, pollInterval, reloadInterval)
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

// processAQIResponse parses the JSON payload received from the WAQI API.
// It extracts the core parameters and handles specific errors, such as
// "Unknown station", to invalidate the city in the database.
func processAQIResponse(city string, body []byte) (*AQIEvent, error) {
	var aqiResp AQIResponse
	if err := json.Unmarshal(body, &aqiResp); err != nil {
		return nil, fmt.Errorf("Error parsing AQI JSON: %w", err)
	}

	if aqiResp.Status == "error" {
		var errorMsg string
		if err := json.Unmarshal(aqiResp.Data, &errorMsg); err == nil {
			if errorMsg == "Unknown station" {
				if err := invalidateCity(city); err != nil {
					log.Printf("[%s] Error marking city as invalid: %v", city, err)
				}
				return nil, fmt.Errorf("unknown station")
			}
			return nil, fmt.Errorf("API error: %s", errorMsg)
		}
		return nil, fmt.Errorf("API returned error status without message")
	}

	if aqiResp.Status != "ok" {
		return nil, fmt.Errorf("API returned non-ok status: %s", aqiResp.Status)
	}

	var dataStr string
	if err := json.Unmarshal(aqiResp.Data, &dataStr); err == nil {
		return nil, fmt.Errorf("API error: %s", dataStr)
	}

	var aqiData AQIData
	if err := json.Unmarshal(aqiResp.Data, &aqiData); err != nil {
		return nil, fmt.Errorf("error parsing AQI data: %w", err)
	}

	event := transformToEvent(city, aqiData)
	return &event, nil
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
	event, err := processAQIResponse(city, body)
	if err != nil {
		log.Printf("[%s] %v", city, err)
		return
	}

	// Send to Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sendToKafka(ctx, *event); err != nil {
		log.Printf("[%s] Error sending to Kafka: %v", city, err)
	} else {
		log.Printf("[%s] Sent to Kafka - AQI: %d", city, event.AQI)
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

func scheduleInitialPoll(cities []City, tasks chan<- string, stopCh <-chan struct{}) {
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
	log.Println("Initial poll scheduled (goroutines launched with jitter)")
}

func handleCitiesReload(citiesPtr *[]City, citiesMutex *sync.RWMutex) {
	log.Println("Reloading cities from database...")
	newCities, err := loadCitiesFromDB()
	if err != nil {
		log.Printf("Failed to reload cities: %v", err)
		return
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
}

// scheduler manages the polling lifecycle.
// It distributes tasks to workers via channels, adding an initial
// jitter to avoid flooding the API with simultaneous requests.
func scheduler(tasks chan<- string, citiesPtr *[]City, citiesMutex *sync.RWMutex, pollInterval, reloadInterval time.Duration, stopCh <-chan struct{}) {
	defer close(tasks)

	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	reloadTicker := time.NewTicker(reloadInterval)
	defer reloadTicker.Stop()

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
		scheduleInitialPoll(currentCities, tasks, stopCh)
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
			handleCitiesReload(citiesPtr, citiesMutex)

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

	_ = godotenv.Load()

	loadConfig()
	initClients()
	initDB()

	// Load cities from database
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
