package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
)

var apiKey string
var apiHostEvents string
var apiHostMedals string

func init() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Load environment variables
	apiKey = os.Getenv("RAPIDAPI_KEY")
	apiHostEvents = os.Getenv("RAPIDAPI_HOST_EVENTS")
	apiHostMedals = os.Getenv("RAPIDAPI_HOST_MEDALS")

	if apiKey == "" || apiHostEvents == "" || apiHostMedals == "" {
		log.Fatal("Missing required environment variables in .env file")
	}
}

// --- Data Structures (JSON Mapping) ---
type Event struct {
	ID           string `json:"id"`
	Discipline   string `json:"discipline"`
	IsMedalEvent bool   `json:"isMedalEvent"`
	Teams        []struct {
		Code string `json:"code"`
	} `json:"teams"`
}

type EventResponse struct {
	Events []Event `json:"events"`
}

type MedalStandings struct {
	Medals map[string]interface{} `json:"medals"` // Dynamic mappging for medal standings
}

func fetchData(url string, host string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("x-rapidapi-key", apiKey)
	req.Header.Add("x-rapidapi-host", host)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

func pollEvents() {
	ticker := time.NewTicker(8 * time.Hour)
	for range ticker.C {
		fmt.Println("[Events] Checking next events...")
		url := "https://" + apiHostEvents + "/events/upcoming?hours=24"
		body, err := fetchData(url, apiHostEvents)
		if err != nil {
			fmt.Println("Error Events API:", err)
			continue
		}

		var resp EventResponse
		json.Unmarshal(body, &resp)

		for _, ev := range resp.Events {
			if ev.IsMedalEvent {
				fmt.Printf(" ALERT: Final coming: %s\n", ev.Discipline)
				//TODO: Send to Kafka
			}
		}
	}
}

func pollMedals() {
	ticker := time.NewTicker(12 * time.Hour)
	for range ticker.C {
		fmt.Println("[Medals] Checking medal standings...")
		url := "https://" + apiHostMedals + "/medals/countries?year=2026"
		_, err := fetchData(url, apiHostMedals) // We can ignore the response for now, just checking if API is reachable
		if err != nil {
			fmt.Println("Error Medals API:", err)
			continue
		}

		//TODO: Process medal standings and send to Kafka
		fmt.Println("Medal standings updated.")
	}
}

// --- MAIN ---
func main() {
	fmt.Println("Starting Poller...")

	go pollEvents()
	go pollMedals()

	select {} // Block main from exiting
}
