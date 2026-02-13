package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
)

var apiKey string
var apiHost string
var kafkaBroker string

func init() {
	// Load .env file
	_ = godotenv.Load()

	// Load environment variables
	apiKey = os.Getenv("AQI_API_KEY")
	apiHost = os.Getenv("AQI_API_HOST")
	kafkaBroker = os.Getenv("KAFKA_BROKERS")
	if apiKey == "" || apiHost == "" {
		log.Fatal("Missing required environment variables: AQI_API_KEY and AQI_API_HOST")
	}

	if kafkaBroker == "" {
		kafkaBroker = "kafka:29092" // Default
	}
}

// --- Data Structures (JSON Mapping) ---
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

func fetchData(url string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := client.Do(req)
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

// Save data to file for debugging
func saveToFile(filename string, data []byte) error {
	if err := os.MkdirAll("./output", 0755); err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func sendToKafka(topic string, event AQIEvent) error {
	conn, err := kafkago.DialLeader(context.Background(), "tcp", kafkaBroker, topic, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = conn.WriteMessages(
		kafkago.Message{
			Key:   []byte(event.City),
			Value: eventJSON,
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

func pollAQI(city string, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	pollCity(city)

	for range ticker.C {
		pollCity(city)
	}
}

func pollCity(city string) {
	fmt.Println("Checking AQI for city:", city)
	url := fmt.Sprintf("https://%s/feed/%s/?token=%s", apiHost, city, apiKey)

	body, err := fetchData(url)
	if err != nil {
		fmt.Printf("[%s] Error fetching AQI: %v\n", city, err)
		return
	}

	// Parse JSON response
	var aqiResp AQIResponse
	if err := json.Unmarshal(body, &aqiResp); err != nil {
		fmt.Printf("[%s] Error parsing AQI JSON: %v\n", city, err)
		return
	}

	if aqiResp.Status != "ok" {
		fmt.Printf("[%s] API returned non-ok status: %s\n", city, aqiResp.Status)
		return
	}

	// Transform to event
	event := transformToEvent(city, aqiResp)

	// Send to Kafka
	if err := sendToKafka("aqi-raw", event); err != nil {
		fmt.Printf("[%s] Error sending to Kafka: %v", city, err)
	} else {
		fmt.Printf("[%s] ✅ Sent to Kafka - AQI: %d\n", city, event.AQI)
	}
}

// --- MAIN ---
func main() {
	fmt.Println("Starting Poller...")

	// List of cities to monitor
	cities := []string{
		"milan",
		"rome",
		"venice",
		"turin",
		"naples",
	}

	var wg sync.WaitGroup

	// Start a goroutine for each city
	for _, city := range cities {
		wg.Add(1)
		go pollAQI(city, &wg)
	}

	// Wait indefinitely (or until all goroutines finish, which they won't in this case)
	wg.Wait()
}
