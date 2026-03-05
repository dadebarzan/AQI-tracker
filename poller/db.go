package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB

// Initialize the database connection
func initDB() {
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "postgres"
	}
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")

	dbSSLMode := os.Getenv("DB_SSLMODE")
	if dbSSLMode == "" {
		dbSSLMode = "disable"
	}

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbUser, dbPass, dbName, dbSSLMode)

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
			return
		}
		log.Printf("Database ping attempt %d/%d failed: %v", i, maxAttempts, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("Database connection failed after %d attempts: %v", maxAttempts, err)
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
