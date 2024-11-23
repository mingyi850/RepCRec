package internal

import (
	"fmt"
	"os"
	"testing"

	"github.com/mingyi850/repcrec/internal"
	"github.com/mingyi850/repcrec/internal/domain"
	"github.com/stretchr/testify/assert"
)

func runTest(filePath string) (*SiteCoordinatorTestImpl, domain.TransactionManager, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}
	defer file.Close()
	siteCoordinator := CreateSiteCoordinatorTestImpl(10)
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

	t.Run("Transactions should re-block when a blocking site is encountered during recovery", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test9.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx2, waiting2, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())

		// After site 4 recovers, Tx2 should write site 4 and wait for site 6
		siteCoordinator.Recover(4, 12)
		transactionManager.Recover(4, 12)
		tx2, waiting2, _ = transactionManager.GetTransaction(2)
		assert.Equal(t, true, waiting2)
		assert.Equal(t, domain.TxWaiting, tx2.GetState())
		result, exists := tx2.GetSiteWrites()[4] // Check that write of variable x3 to site 4 happened locally
		fmt.Println("Site Writes", result)
		assert.Equal(t, true, exists)

		// After site 6 recovers, Tx2 should write to site 6 and commit
		siteCoordinator.Recover(6, 13)
		transactionManager.Recover(6, 13)
		tx2, waiting2, _ = transactionManager.GetTransaction(2)
		assert.Equal(t, false, waiting2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		transactionManager.Begin(3, 14)
		read, err := transactionManager.Read(3, 5, 15)
		assert.Nil(t, err)
		assert.Equal(t, 444, read.Value) // Should read last value written by Tx2

	})

	t.Run("Failure after write aborts transaction", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test10.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		assert.Equal(t, 40, siteCoordinator.GetLatestValue(1, 4).GetValue()) // Original value of 4
	})

	t.Run("RWRW in graph cycle aborts transaction", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test11.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxAborted, tx3.GetState())

		assert.Equal(t, 222, siteCoordinator.GetLatestValue(1, 4).GetValue()) // Tx writes to x4
		assert.Equal(t, 30, siteCoordinator.GetLatestValue(4, 3).GetValue())  // Tx writes to x4
		assert.Equal(t, 111, siteCoordinator.GetLatestValue(6, 5).GetValue()) // Tx writes to x4

	})

	t.Run("RWRW in graph cycle aborts transaction part 2", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test12.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
		tx4, _, _ := transactionManager.GetTransaction(4)
		assert.Equal(t, domain.TxAborted, tx4.GetState())
	})

	t.Run("RWRW in graph cycle - abort avoided by strategic commits", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test14.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
		tx4, _, _ := transactionManager.GetTransaction(4)
		assert.Equal(t, domain.TxCommitted, tx4.GetState())

	})

	t.Run("Transaction should abort on read if no valid sites (even if active)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test15.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxAborted, tx3.GetState())
	})

	t.Run("Transaction should abort if another commits first", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test16.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxAborted, tx2.GetState())
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
	})

	t.Run("Write conflict between T1 and T2", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test17.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 201, siteCoordinator.GetLatestValue(2, 1).GetValue())
		assert.Equal(t, 202, siteCoordinator.GetLatestValue(4, 2).GetValue())
	})

	t.Run("Serializable snapshot - no conflicts", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test18.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 101, siteCoordinator.GetLatestValue(2, 1).GetValue())
		assert.Equal(t, 102, siteCoordinator.GetLatestValue(4, 2).GetValue())
	})

	t.Run("All transaction commits despite site failure", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test19.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 80, siteCoordinator.GetLatestValue(2, 8).GetValue())
		assert.Equal(t, 88, siteCoordinator.GetLatestValue(4, 8).GetValue())
	})

	t.Run("Write is lost due to abort", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test20.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxAborted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 40, siteCoordinator.GetLatestValue(2, 4).GetValue())
		assert.Equal(t, 91, siteCoordinator.GetLatestValue(4, 4).GetValue())
	})

	t.Run("Write is lost due to abort (part 2)", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test21.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxAborted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 91, siteCoordinator.GetLatestValue(2, 4).GetValue())
		assert.Equal(t, 91, siteCoordinator.GetLatestValue(4, 4).GetValue())
	})

	t.Run("Write is lost due to abort (part 3)", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test22.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		siteCoordinator.Dump()
		assert.Equal(t, 80, siteCoordinator.GetLatestValue(2, 8).GetValue())
		assert.Equal(t, 88, siteCoordinator.GetLatestValue(4, 8).GetValue())
	})

	t.Run("Write is lost due to abort (part 4)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test23.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
	})

	t.Run("Read from unreplicated variable at recovering site is allowed", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test24.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, 80, siteCoordinator.GetLatestValue(3, 8).GetValue())
		assert.Equal(t, 80, siteCoordinator.GetLatestValue(4, 8).GetValue())
		assert.Equal(t, 88, siteCoordinator.GetLatestValue(5, 8).GetValue())
	})

	t.Run("Snapshot isolation reads from original version of site at transaction begin", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test25.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		result, _ := transactionManager.Read(2, 3, 10)
		assert.Equal(t, 30, result.Value)
	})

	t.Run("Snapshot isolation reads from original version of site at transaction begin (part 2)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test26.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx2Result, _ := transactionManager.Read(2, 3, 11)
		assert.Equal(t, 30, tx2Result.Value)
		tx3Result, _ := transactionManager.Read(3, 3, 10)
		assert.Equal(t, 33, tx3Result.Value)
	})

	t.Run("Snapshot isolation reads from original version of site at transaction begin (part 3)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test27.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx3Read, _ := transactionManager.Read(3, 4, 10)
		assert.Equal(t, 40, tx3Read.Value)
		transactionManager.End(2, 11)
		transactionManager.End(3, 12)
		tx1Read, _ := transactionManager.Read(1, 2, 13)
		assert.Equal(t, 20, tx1Read.Value)
	})

	t.Run("Snapshot isolation reads from new version of site at transaction begin.", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test28.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx3Read, _ := transactionManager.Read(3, 4, 10)
		assert.Equal(t, 40, tx3Read.Value)
		transactionManager.End(2, 11)
		transactionManager.End(3, 12)
		transactionManager.Begin(1, 13)
		tx1Read, _ := transactionManager.Read(1, 2, 14)
		assert.Equal(t, 22, tx1Read.Value)
	})

	t.Run("All transactions commit if no conflict occurs", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test29.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
	})

	t.Run("All transactions commit if no conflict occurs (part 2)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test30.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
	})

	t.Run("Only first commit wins", func(t *testing.T) {
		siteManager, transactionManager, err := runTest("resources/test31.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		assert.Equal(t, domain.TxAborted, tx2.GetState())
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
		assert.Equal(t, 10, siteManager.GetLatestValue(5, 2).GetValue())
	})

	t.Run("Only first commit wins (part 2)", func(t *testing.T) {
		siteManager, transactionManager, err := runTest("resources/test32.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxAborted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
		assert.Equal(t, 20, siteManager.GetLatestValue(5, 2).GetValue())
	})

	t.Run("Complex case - transasction aborts due to failure, then first commit wins", func(t *testing.T) {
		siteManager, transactionManager, err := runTest("resources/test33.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		tx4, _, _ := transactionManager.GetTransaction(4)
		tx5, _, _ := transactionManager.GetTransaction(5)
		assert.Equal(t, domain.TxAborted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
		assert.Equal(t, domain.TxAborted, tx4.GetState())
		assert.Equal(t, domain.TxAborted, tx5.GetState())
		assert.Equal(t, 44, siteManager.GetLatestValue(5, 4).GetValue())
	})

	t.Run("Snapshot isolation - reads value from when transaction began", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test34.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		t3Read, _ := transactionManager.Read(3, 4, 10)
		transactionManager.End(2, 11)
		transactionManager.End(3, 12)
		transactionManager.Begin(1, 13)
		t1Read, _ := transactionManager.Read(1, 2, 14)
		assert.Equal(t, 22, t1Read.Value)
		assert.Equal(t, 40, t3Read.Value)
	})

	t.Run("Snapshot isolation - reads value from when transaction began. Ignore aborted writes", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test35.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		t3Read, _ := transactionManager.Read(3, 3, 10)
		transactionManager.End(2, 11)
		siteCoordinator.Fail(4, 12)
		transactionManager.End(3, 13)
		transactionManager.Begin(1, 14)
		t1Read, _ := transactionManager.Read(1, 2, 15)
		assert.Equal(t, 20, t1Read.Value)
		assert.Equal(t, 30, t3Read.Value)
	})

	t.Run("Circular conflict - all RW edges. Cycle closing transaction aborted", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test36.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		tx4, _, _ := transactionManager.GetTransaction(4)
		tx5, _, _ := transactionManager.GetTransaction(5)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
		assert.Equal(t, domain.TxCommitted, tx4.GetState())
		assert.Equal(t, domain.TxAborted, tx5.GetState())
	})

	t.Run("Almost Circular conflict - all RW edges. No cycle because a transaction aborts", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test37.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		tx4, _, _ := transactionManager.GetTransaction(4)
		tx5, _, _ := transactionManager.GetTransaction(5)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
		assert.Equal(t, domain.TxCommitted, tx4.GetState())
		assert.Equal(t, domain.TxCommitted, tx5.GetState())
	})

	t.Run("Write conflcit, first commit wins", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test38.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxAborted, tx2.GetState())
	})

	t.Run("Simple R-W cycle - cycle closing transaction aborts", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test39.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxAborted, tx2.GetState())
	})

	t.Run("R-W cycle - cycle closing transaction with WW aborts", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test40.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
	})

	t.Run("Read should abort immediately if no valid site for read on replicated site", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test41.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
	})

	t.Run("Read should abort immediately if no valid site for read on replicated site (part 2)", func(t *testing.T) {
		_, transactionManager, err := runTest("resources/test42.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		tx4, _, _ := transactionManager.GetTransaction(4)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxAborted, tx3.GetState())
		assert.Equal(t, domain.TxCommitted, tx4.GetState())
	})

	t.Run("Read should wait if valid site exists but is down for read on replicated site", func(t *testing.T) {
		siteCoordinator, transactionManager, err := runTest("resources/test43.txt")
		if err != nil {
			fmt.Printf("Error: %v", err)
			t.Fatal(err)
		}
		tx1, _, _ := transactionManager.GetTransaction(1)
		tx2, _, _ := transactionManager.GetTransaction(2)
		tx3, _, _ := transactionManager.GetTransaction(3)
		tx4, _, _ := transactionManager.GetTransaction(4)
		assert.Equal(t, domain.TxCommitted, tx1.GetState())
		assert.Equal(t, domain.TxCommitted, tx2.GetState())
		assert.Equal(t, domain.TxCommitted, tx4.GetState())
		assert.Equal(t, domain.TxWaiting, tx3.GetState())
		siteCoordinator.Recover(2, 24)
		transactionManager.Recover(2, 24)
		tx3Read, _ := transactionManager.Read(3, 8, 25)
		assert.Equal(t, 88, tx3Read.Value)
		transactionManager.End(3, 26)
		tx3, _, _ = transactionManager.GetTransaction(3)
		assert.Equal(t, domain.TxCommitted, tx3.GetState())
	})
}
