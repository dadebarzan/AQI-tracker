package main

import (
	"context"
	"encoding/json"
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
	pollInterval = time.Duration(getEnvAsInt("POLL_INTERVAL_SECONDS", 30)) * time.Minute
	reloadInterval = time.Duration(getEnvAsInt("RELOAD_INTERVAL_SECONDS", 60)) * time.Minute

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

	_ = godotenv.Load()

	rand.New(rand.NewSource(time.Now().UnixNano()))

	loadConfig()
	initClients()
	initDB()

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
