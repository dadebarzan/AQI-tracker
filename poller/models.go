package main

import (
	"encoding/json"
	"time"
)

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
