package poller

import (
	"database/sql"
	"fmt"
	"log"
)

var db *sql.DB

func init() {
	// TODO
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
