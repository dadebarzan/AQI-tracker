-- Create Cities Table
-- Cities names are not unique globally, but the API cannot distinct them
-- just by their name, so we will assume that they are unique for simplicity.
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT NULL,
    country VARCHAR(64),
    continent VARCHAR(32),
    valid_entry BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cities_name ON cities(name);

-- Create AQI_History Table
CREATE TABLE IF NOT EXISTS aqi_history (
    id SERIAL PRIMARY KEY,
    city VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    aqi INTEGER NOT NULL,
    co DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    pm25 DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    timestamp TIMESTAMP NOT NULL,
    CONSTRAINT aqi_data_city_timestamp_key UNIQUE (city, timestamp)
);

CREATE INDEX idx_aqi_history_city ON aqi_history(city);
CREATE INDEX idx_aqi_history_timestamp ON aqi_history(timestamp);


