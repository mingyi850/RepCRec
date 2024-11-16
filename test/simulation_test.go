package internal

import (
	"fmt"
	"os"
	"testing"

	"github.com/mingyi850/repcrec/internal"
	"github.com/mingyi850/repcrec/internal/domain"
	test "github.com/mingyi850/repcrec/test/domain"
	"github.com/stretchr/testify/assert"
)

func runTest(filePath string) (*test.SiteCoordinatorTestImpl, domain.TransactionManager, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}
	defer file.Close()
	siteCoordinator := test.CreateSiteCoordinatorTestImpl(10)
	transactionManager := domain.CreateTransactionManager(siteCoordinator)
	err = internal.Simulation(file, siteCoordinator, transactionManager)
	return siteCoordinator, transactionManager, err
}
func TestSimulation(t *testing.T) {

	t.Run("Successfully Reads and Writes to unreplicated site", func(t *testing.T) {
		siteCoordinator, _, err := runTest("resources/test1.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		result := siteCoordinator.GetLatestValue(4, 3) // Site 4, Key 3
		assert.Equal(t, 111, result.GetValue())
	})

	t.Run("Successfully Reads and Writes to replicated site", func(t *testing.T) {
		siteCoordinator, _, err := runTest("resources/test2.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		sites := siteCoordinator.GetSitesForKey(4)
		for _, site := range sites {
			result := siteCoordinator.GetLatestValue(site, 4)
			assert.Equal(t, 111, result.GetValue())
		}
	})

	t.Run("Should terminate on invalid operation", func(t *testing.T) {
		siteManager, _, err := runTest("resources/test3.txt")
		if err != nil {
			assert.Contains(t, err.Error(), "does not exist")
			assert.Equal(t, 40, siteManager.GetLatestValue(1, 4).GetValue()) // Original value of 4

		} else {
			t.Fatal("Expected error to be thrown")
		}

	})

	t.Run("First Commit Wins", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test4.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		sites := siteCoordinator.GetSitesForKey(3)
		for _, site := range sites {
			result := siteCoordinator.GetLatestValue(site, 3)
			assert.Equal(t, 222, result.GetValue())
			transaction, _, _ := transactionManager.GetTransaction(1)
			assert.Equal(t, transaction.GetState(), domain.TxAborted)
		}
	})

	t.Run("Reads should last committed value at transaction start", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test5.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		result1, _ := transactionManager.Read(3, 4, 10)
		assert.Equal(t, 111, result1.Value) // Need fix
		result2, _ := transactionManager.Read(4, 4, 10)
		assert.Equal(t, 222, result2.Value)
	})

	t.Run("Reads should abort if no site can possibly service request and wait if there is a site, but it is down", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test6.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx2, waiting2, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())

		tx3, waiting3, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, false, waiting3)
		assert.Equal(t, domain.TxAborted, tx3.GetState())

	})

	t.Run("Reads should always wait for site on unreplicated variable", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test7.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx2, waiting2, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())

		tx3, waiting3, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, true, waiting3)
		assert.Equal(t, domain.TxWaiting, tx3.GetState())

	})

	t.Run("Transactions should continue when a blocking site is recovered", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test6.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx2, waiting2, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())

		// Also, Tx2 should recover afterwards when valid site is recovered
		siteCoordinator.Recover(8, 10)    // Invalid site - Tx2 should be waiting on site 10
		transactionManager.Recover(8, 10) // Invalid site - Tx2 should be waiting on site 10
		tx2, waiting2, _ = transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())

		siteCoordinator.Recover(10, 10) // Valid site - Tx2 should be able to continue
		transactionManager.Recover(10, 10)
		tx2, waiting2, _ = transactionManager.GetTransaction(2)
		assert.Equal(t, false, waiting2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())

	})

}
