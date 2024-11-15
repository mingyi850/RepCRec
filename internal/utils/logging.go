package utils

import (
	"fmt"
)

func LogRead(transaction int, key int, value int) {
	fmt.Printf("x%d: %d\n", key, value)
}

func LogAbort(transaction int, reason string) {
	if reason == "" {
		fmt.Printf("T%d aborts\n", transaction)
	} else {
		fmt.Printf("T%d aborts: %s\n", transaction, reason)
	}
}

func LogWait(transaction int) {
	fmt.Printf("T%d waits\n", transaction)
}

func LogWaiting(transaction int) {
	fmt.Printf("T%d waiting\n", transaction)
}

func LogCommit(transaction int) {
	fmt.Printf("T%d commits\n", transaction)
}

func LogWrite(transaction int, key int, sites []int) {
	fmt.Printf("T%d - x%d: %v\n", transaction, key, sites)
}
