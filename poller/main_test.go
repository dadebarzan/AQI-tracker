package main

import (
	"strings"
	"testing"
)

func createTests() []struct {
	name        string
	city        string
	input       []byte
	wantEvent   *AQIEvent
	wantErr     bool
	errContains string
} {
	return []struct {
		name        string
		city        string
		input       []byte
		wantEvent   *AQIEvent
		wantErr     bool
		errContains string
	}{
		{
			name: "success - valid response maps all fields",
			city: "milano",
			input: []byte(`{
                "status": "ok",
                "data": {
                    "aqi": 42,
                    "idx": 1234,
                    "city": {
                        "geo": [45.4642, 9.19],
                        "name": "Milano",
                        "url": "https://example.com",
                        "location": "Lombardy"
                    },
                    "dominentpol": "pm25",
                    "iaqi": {
                        "co":   {"v": 3.5},
                        "no2":  {"v": 12.0},
                        "pm10": {"v": 25.0},
                        "pm25": {"v": 42.0},
                        "t":    {"v": 18.5}
                    },
                    "time": {
                        "s":   "2026-03-08 12:00:00",
                        "tz":  "+01:00",
                        "v":   1773064800,
                        "iso": "2026-03-08T12:00:00+01:00"
                    }
                }
            }`),
			wantEvent: &AQIEvent{
				City:        "milano",
				AQI:         42,
				CO:          3.5,
				NO2:         12.0,
				PM10:        25.0,
				PM25:        42.0,
				Temperature: 18.5,
				Timestamp:   1773064800,
				Latitude:    45.4642,
				Longitude:   9.19,
			},
		},
		{
			name:        "error - Unknown station marks city invalid",
			city:        "invalidcity",
			input:       []byte(`{"status":"error","data":"Unknown station"}`),
			wantErr:     true,
			errContains: "unknown station",
		},
		{
			name:        "error - generic API error message",
			city:        "somecity",
			input:       []byte(`{"status":"error","data":"Rate limit exceeded"}`),
			wantErr:     true,
			errContains: "API error: Rate limit exceeded",
		},
	}
}

func validateAQIResponse(t *testing.T, tt struct {
	name        string
	city        string
	input       []byte
	wantEvent   *AQIEvent
	wantErr     bool
	errContains string
}, event *AQIEvent, err error) {
	if tt.wantErr {
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
			t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
		}
		if event != nil {
			t.Errorf("expected nil event on error, got %+v", event)
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event, got nil")
	}
	if *event != *tt.wantEvent {
		t.Errorf("event = %+v, want %+v", *event, *tt.wantEvent)
	}
}

func TestProcessAQIResponse(t *testing.T) {
	originalInvalidate := invalidateCity
	invalidateCity = func(city string) error {
		return nil // Do nothing during tests
	}

	t.Cleanup(func() {
		invalidateCity = originalInvalidate
	})

	tests := createTests()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := processAQIResponse(tt.city, tt.input)
			validateAQIResponse(t, tt, event, err)
		})
	}
}
