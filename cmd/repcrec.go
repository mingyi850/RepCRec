/**************************
File: simulation.go
Author: Mingyi Lim
Description: This file contains the entrypoint to the program. It reads the input file and starts the simulation.
***************************/

package main

import (
	"fmt"
	"os"

	"github.com/mingyi850/repcrec/internal"
	"github.com/mingyi850/repcrec/internal/domain"
)

/*
************
Runs Main function

If filename is provided, reads instructions from file
Else, reads instructions from stdin
************
*/
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
	siteCoordinator := domain.CreateSiteCoordinator(10)
	transactionManager := domain.CreateTransactionManager(siteCoordinator)
	err = internal.Simulation(file, siteCoordinator, transactionManager)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Completed Successfully")
}
