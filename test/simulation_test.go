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

}
