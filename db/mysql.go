package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type DB struct {
	conn *sql.DB
}

func NewDB(configFile string) (*DB, error) {
	// Read the JSON config file
	configData, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	// Parse the JSON config
	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		return nil, err
	}

	// Create the MySQL connection string
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.Username, config.Password, config.Host, config.Port, config.Database)

	// Open a connection to the MySQL database
	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}

	// Ping the database to check if the connection is successful
	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Write(data []string) error {
	// Build the SQL query
	query := "INSERT INTO table_name ("
	values := "VALUES ("
	args := make([]interface{}, 0, len(data))
	i := 0
	for _, value := range data {
		if i > 0 {
			query += ", "
			values += ", "
		}
		query += "?"
		values += "?"
		args = append(args, value)
		i++
	}
	query += ") " + values + ")"

	// Execute the SQL query
	_, err := db.conn.Exec(query, args...)
	if err != nil {
		return err
	}

	return nil
}
