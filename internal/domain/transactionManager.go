package domain

import (
	"fmt"

	"github.com/mingyi850/repcrec/internal/utils"
)

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
	waitingSites        map[int]bool
}

type CommitResult struct {
	success bool
	waiting bool
}

func (result CommitResult) String() string {
	if result.success {
		return "commits"
	}
	return "aborts"
}

type ReadResultType string

const (
	Abort   ReadResultType = "abort"
	Wait    ReadResultType = "wait"
	Success ReadResultType = "success"
	Waiting ReadResultType = "waiting"
)

type ReadResult struct {
	Value      int
	ResultType ReadResultType
}

type TransactionManager interface {
	Begin(tx int, time int) error
	End(tx int, time int) (CommitResult, error) // Either "commit" or "abort"
	Write(tx int, key int, value int, time int) error
	Read(tx int, key int, time int) (ReadResult, error) // Returns read value if available
	Recover(site int, time int) error
}

type TransactionManagerImpl struct {
	SiteCoordinator     SiteCoordinator
	TransactionMap      map[int]*Transaction
	WaitingTransactions map[int]bool
	TransactionGraph    map[int][]int
}

func CreateTransactionManager(SiteCoordinator SiteCoordinator) TransactionManager {
	return &TransactionManagerImpl{
		SiteCoordinator:     SiteCoordinator,
		TransactionMap:      make(map[int]*Transaction),
		WaitingTransactions: make(map[int]bool),
		TransactionGraph:    make(map[int][]int),
	}
}

func (t *TransactionManagerImpl) Begin(tx int, time int) error {
	t.TransactionMap[tx] = &Transaction{
		id:                tx,
		startTime:         time,
		siteWrites:        make(map[int]Operation),
		pendingOperations: make([]Operation, 0),
		waitingSites:      make(map[int]bool),
	}
	t.TransactionGraph[tx] = []int{}
	return nil
}

func (t *TransactionManagerImpl) End(tx int, time int) (CommitResult, error) {
	_, waiting, err := t.getTransaction(tx)
	if err != nil {
		return CommitResult{false, false}, err
	}
	if waiting {
		t.appendWaitingOperation(tx, Operation{End, 0, 0, time})
		return CommitResult{false, true}, nil
	}
	commitSuccess := true
	return CommitResult{commitSuccess, false}, nil
}

func (t *TransactionManagerImpl) Write(tx int, key int, value int, time int) error {
	return nil
}

func (t *TransactionManagerImpl) Read(tx int, key int, time int) (ReadResult, error) {
	transaction, waiting, err := t.getTransaction(tx)
	if err != nil {
		return ReadResult{-1, Abort}, err
	}
	if waiting {
		t.appendWaitingOperation(tx, Operation{Read, key, 0, time})
		fmt.Println("Appended waiting transaction", tx)
		return ReadResult{-1, Waiting}, nil
	}
	siteList := t.SiteCoordinator.GetValidSitesForRead(key, transaction.startTime)
	if len(siteList) == 0 {
		// Add Abort logic here
		return ReadResult{-1, Abort}, nil
	}
	for _, site := range siteList {
		value, err := t.SiteCoordinator.ReadActiveSite(site, key, time)
		if err == nil {
			t.appendCompletedOperation(tx, Operation{Read, key, value.value, time})
			return ReadResult{value.value, Success}, nil
		}
	}
	// Wait transaction logic
	err = t.waitTransaction(tx, siteList)
	return ReadResult{-1, Wait}, err
}

func (t *TransactionManagerImpl) Recover(site int, time int) error {
	for tx := range t.WaitingTransactions {
		fmt.Println("Waiting transaction", tx, "on sites", utils.GetMapKeys(t.TransactionMap[tx].waitingSites))
		transaction, waiting, err := t.getTransaction(tx)
		if err != nil {
			return err
		}
		if !waiting {
			return fmt.Errorf("Transaction %d is not waiting", tx)
		}
		if _, exists := transaction.waitingSites[site]; exists {
			// Run all pending operations on site
			err = t.unwaitTransaction(tx)
			if err != nil {
				return err
			}
			err = t.runPendingOperations(transaction, time)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *TransactionManagerImpl) runPendingOperations(tx *Transaction, recoverTime int) error {
	for index, operation := range tx.pendingOperations {
		switch operation.operationType {
		case Write:
			err := t.Write(tx.id, operation.key, operation.value, recoverTime)
			if err != nil {
				return err
			}
		case Read:
			value, err := t.Read(tx.id, operation.key, recoverTime)
			if err != nil {
				return err
			}
			HandleReadResult(tx.id, operation.key, value)
			if value.ResultType != Success {
				t.truncatePendingOperations(tx, index)
				return nil
			}
		case End:
			result, err := t.End(tx.id, recoverTime)
			if err != nil {
				return err
			}
			HandleCommitResult(tx.id, result)
		}
	}
	return nil
}

func (t *TransactionManagerImpl) truncatePendingOperations(tx *Transaction, index int) {
	tx.pendingOperations = tx.pendingOperations[index:]
}

func (t *TransactionManagerImpl) getTransaction(tx int) (*Transaction, bool, error) {
	transaction, exists := t.TransactionMap[tx]
	if !exists {
		return &Transaction{}, false, fmt.Errorf("Transaction %d does not exist", tx)
	}
	_, waiting := t.WaitingTransactions[tx]
	return transaction, waiting, nil
}

func (t *TransactionManagerImpl) waitTransaction(tx int, sites []int) error {
	transaction, waiting, err := t.getTransaction(tx)
	if err != nil {
		return err
	}
	if waiting {
		return nil
	}
	for _, site := range sites {
		transaction.waitingSites[site] = true
	}
	t.WaitingTransactions[tx] = true
	return nil
}

func (t *TransactionManagerImpl) unwaitTransaction(tx int) error {
	transaction, waiting, err := t.getTransaction(tx)
	if err != nil {
		return err
	}
	if !waiting {
		return fmt.Errorf("Transaction %d is not waiting", tx)
	}
	transaction.waitingSites = make(map[int]bool)
	delete(t.WaitingTransactions, tx)
	return nil
}

func (t *TransactionManagerImpl) appendWaitingOperation(tx int, operation Operation) error {
	transaction, waiting, err := t.getTransaction(tx)
	if err != nil {
		return err
	}
	if !waiting {
		return fmt.Errorf("Transaction %d is not waiting", tx)
	}
	transaction.pendingOperations = append(transaction.pendingOperations, operation)
	return nil
}

func (t *TransactionManagerImpl) appendCompletedOperation(tx int, operation Operation) error {
	transaction, waiting, err := t.getTransaction(tx)
	if err != nil {
		return err
	}
	if waiting {
		return fmt.Errorf("Transaction %d is waiting", tx)
	}
	transaction.completedOperations = append(transaction.completedOperations, operation)
	return nil
}

func HandleReadResult(tx int, key int, result ReadResult) {
	switch result.ResultType {
	case Success:
		utils.LogRead(tx, key, result.Value)
	case Abort:
		utils.LogAbort(tx)
	case Wait:
		utils.LogWait(tx)
	case Waiting:
		utils.LogWaiting(tx)
	}
}

func HandleCommitResult(tx int, result CommitResult) {
	if result.waiting {
		utils.LogWaiting(tx)
		return
	}
	if result.success {
		utils.LogCommit(tx)
	} else {
		utils.LogAbort(tx)
	}
}

/*

Rough logic for read (inTxManager) ->
1. GetValidSitesForKey -> Give us a list of all valid sites we could read from.
-> If none, Abort
1. Find any active site from the list. If found, read from there.
2. If none, wait transaction
-- Done

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

Recover:
1. For each waiting site, check if we can continue.
2. If yes, run all operations on site in pendingOperations.
*/
