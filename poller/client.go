package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

var apiKey string
var apiHost string
var kafkaBroker string
var kafkaWriter *kafkago.Writer
var httpClient *http.Client

func init() {
	// TODO
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
