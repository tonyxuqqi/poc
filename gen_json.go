package main

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// Person represents the JSON structure
type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	Job  string `json:"Job"`
}

func main() {
	// Seed the random number generator with current time
	rand.Seed(time.Now().UnixNano())

	// Open a CSV file for writing
	file, err := os.Create("output.csv")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	id := 0
	err = writer.Write([]string{"ID", "JSON"})
	if err != nil {
		log.Fatalf("Error writing CSV header: %v", err)
	}

	// Generate random JSON data and write to CSV
	for i := 0; i < 1000000; i++ { // Generate 10 random records
		id++
		// Generate random data for JSON fields
		name := getRandomName()
		age := getRandomAge()
		job := getRandomJob()

		// Create Person struct with random data
		person := Person{Name: name, Age: age, Job: job}

		// Marshal Person struct into JSON
		jsonData, err := json.Marshal(person)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			continue
		}

		// Write JSON string to CSV
		err = writer.Write([]string{strconv.Itoa(id), string(jsonData)})
		if err != nil {
			log.Printf("Error writing to CSV: %v", err)
			continue
		}
	}

	log.Println("CSV file generated successfully")
}

// Helper function to generate random name
func getRandomName() string {
	names := []string{"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry", "Ivy", "Jack"}
	return names[rand.Intn(len(names))]
}

// Helper function to generate random age between 20 and 60
func getRandomAge() int {
	return rand.Intn(41) + 20
}

// Helper function to generate random job
func getRandomJob() string {
	jobs := []string{"Engineer", "Teacher", "Doctor", "Artist", "Chef", "Programmer", "Designer"}
	return jobs[rand.Intn(len(jobs))]
}
