package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchData(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantErr    bool
	}{
		{
			name:       "success - HTTP 200 with valid JSON payload",
			statusCode: http.StatusOK,
			body:       `{"status":"ok","data":{"aqi":50}}`,
		},
		{
			name:       "error - HTTP 500 Internal Server Error",
			statusCode: http.StatusInternalServerError,
			body:       "Internal Server Error",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			httpClient = server.Client()

			body, err := fetchData(server.URL)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(body) != tt.body {
				t.Errorf("body = %q, want %q", string(body), tt.body)
			}
		})
	}
}
