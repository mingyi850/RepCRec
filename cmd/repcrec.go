package main

import (
	"fmt"
	"os"

	"github.com/mingyi850/repcrec/internal"
	"github.com/mingyi850/repcrec/internal/domain"
)

func main() {
	file := os.Stdin
	var err error
	if len(os.Args) >= 2 {
		filename := os.Args[1]
		fmt.Printf("Opening file %s\n", filename)
		file, err = os.Open(filename)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer file.Close()
	}
	if file == os.Stdin {
		fmt.Println("Please enter input and press Ctrl-D or enter exit to exit")
	}
	// Read file line by line
	siteCoordinator := domain.CreateSiteCoordinator(10)
	transactionManager := domain.CreateTransactionManager(siteCoordinator)
	err = internal.Simulation(file, siteCoordinator, transactionManager)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Completed Successfully")
}
