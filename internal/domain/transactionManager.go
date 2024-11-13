package domain

import "fmt"

type CommitResult struct {
	success bool
}

type OperationType string

const (
	Write OperationType = "write"
	Read  OperationType = "read"
	End   OperationType = "end"
)

type Operation struct {
	operationType OperationType
	key           int
	value         int
	time          int
}

type Transaction struct {
	id                  int
	startTime           int
	siteWrites          map[int]Operation
	pendingOperations   []Operation
	completedOperations []Operation
}

func (result CommitResult) String() string {
	if result.success {
		return "commits"
	}
	return "aborts"
}

type TransactionManager interface {
	Begin(tx int, time int) error
	End(tx int, time int) (CommitResult, error) // Either "commit" or "abort"
	Write(tx int, key int, value int, time int) error
	Read(tx int, key int, time int) (int, error) // Returns read value if available
	Recover(site int, time int) error
}

type TransactionManagerImpl struct {
	SiteCoordinator     SiteCoordinator
	TransactionMap      map[int]Transaction
	WaitingTransactions map[int]bool
	TransactionGraph    map[int][]int
}

func CreateTransactionManager(SiteCoordinator SiteCoordinator) TransactionManager {
	return &TransactionManagerImpl{
		SiteCoordinator:     SiteCoordinator,
		TransactionMap:      make(map[int]Transaction),
		WaitingTransactions: make(map[int]bool),
		TransactionGraph:    make(map[int][]int),
	}
}

func (t *TransactionManagerImpl) Begin(tx int, time int) error {
	t.TransactionMap[tx] = Transaction{
		id:                tx,
		startTime:         0,
		siteWrites:        make(map[int]Operation),
		pendingOperations: make([]Operation, 0),
	}
	t.TransactionGraph[tx] = []int{}
	return nil
}

func (t *TransactionManagerImpl) End(tx int, time int) (CommitResult, error) {
	return CommitResult{true}, nil
}

func (t *TransactionManagerImpl) Write(tx int, key int, value int, time int) error {
	return nil
}

func (t *TransactionManagerImpl) Read(tx int, key int, time int) (int, error) {
	siteList := t.SiteCoordinator.GetValidSitesForRead(key, time)
	fmt.Println(siteList)
	return 0, nil
}

func (t *TransactionManagerImpl) Recover(site int, time int) error {
	return nil
}

/*

Rough logic for read (inTxManager) ->
1. GetValidSitesForKey -> Give us a list of all valid sites we could read from.
-> If none, Abort
1. Find any active site from the list. If found, read from there.
2. If none, wait transaction

For Writes:
1. GetActiveSiteFoeKey -> Give us a list of all active sites we can write to.
2. If none: Wait
3. If some -> Write to only that site.

For Commit:
1. Check all writes made by transaction.
2. For each write (sites, key, value, time)
	1. For each site
		1. Make sure site has been up since the write time.
		2. Read last committed value for the key
		3. Make sure last committed value timestamp is less than the write.
	If any of the above fails, abort.
3. Check for cycles in Transaction Graph. If cycle is found, abort.
	If all pass
		1. Commit all writes

Aborts:
1. Remove transaction from TransactionMap
2. Remove transaction from TransactionGraph - set all outgoing edges to nil

*/
