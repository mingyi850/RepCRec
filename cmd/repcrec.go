package main

import (
	"fmt"
	"os"

	"github.com/mingyi850/repcrec/internal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: repcrec <path>")
		os.Exit(1)
	}
	filename := os.Args[1]
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()
	// Read file line by line
	error := internal.Simulation(file)
	if error != nil {
		fmt.Println(error)
	}
	fmt.Println("Completed Successfully")
}
