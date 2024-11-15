package main

import (
	"fmt"
	"os"

	"github.com/mingyi850/repcrec/internal"
	"github.com/mingyi850/repcrec/internal/domain"
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
	siteCoordinator := domain.CreateSiteCoordinator(10)
	transactionManager := domain.CreateTransactionManager(siteCoordinator)
	error := internal.Simulation(file, siteCoordinator, transactionManager)
	if error != nil {
		fmt.Println(error)
	}
	fmt.Println("Completed Successfully")
}
