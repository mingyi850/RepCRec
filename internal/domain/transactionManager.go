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

type ConflictType int

const (
	WW ConflictType = 1
	WR ConflictType = 2
	RW ConflictType = 3
)

type Operation struct {
	operationType OperationType
	key           int
	value         int
	time          int
}

type OperationResultType string

const (
	Abort   OperationResultType = "abort"
	Wait    OperationResultType = "wait"
	Success OperationResultType = "success"
	Waiting OperationResultType = "waiting"
	Aborted OperationResultType = "aborted"
)

type CommitResultReason string

const (
	CommitSiteDown  CommitResultReason = "down"
	CommitSiteStale CommitResultReason = "stale"
	RWCycle         CommitResultReason = "rw_cycle"
	CommitOK        CommitResultReason = "ok"
)

type CommitResult struct {
	ResultType OperationResultType
	reason     string
}

type WriteResult struct {
	ResultType OperationResultType
	Sites      []int
}

type ReadResult struct {
	Value      int
	ResultType OperationResultType
}

type TransactionState string

const (
	TxActive    TransactionState = "active"
	TxWaiting   TransactionState = "waiting"
	TxAborted   TransactionState = "aborted"
	TxCommitted TransactionState = "committed"
)

type Transaction struct {
	id                  int
	startTime           int
	siteWrites          map[int]Operation
	pendingOperations   []Operation
	completedOperations map[int][]Operation
	waitingSites        map[int]bool
	state               TransactionState
}

type TransactionManager interface {
	Begin(tx int, time int) error
	End(tx int, time int) (CommitResult, error) // Either "commit" or "abort"
	Write(tx int, key int, value int, time int) (WriteResult, error)
	Read(tx int, key int, time int) (ReadResult, error) // Returns read value if available
	Recover(site int, time int) error
	GetTransaction(tx int) (*Transaction, bool, error)
}

type TransactionManagerImpl struct {
	SiteCoordinator     SiteCoordinator
	TransactionMap      map[int]*Transaction
	WaitingTransactions map[int]bool
	TransactionGraph    TransactionGraph
}

func CreateTransactionManager(SiteCoordinator SiteCoordinator) *TransactionManagerImpl {
	return &TransactionManagerImpl{
		SiteCoordinator:     SiteCoordinator,
		TransactionMap:      make(map[int]*Transaction),
		WaitingTransactions: make(map[int]bool),
		TransactionGraph:    CreateTransactionGraph(),
	}
}

/*
************
Transaction Manager Methods
************
*/
func (t *TransactionManagerImpl) Begin(tx int, time int) error {
	t.TransactionMap[tx] = &Transaction{
		id:                  tx,
		startTime:           time,
		siteWrites:          make(map[int]Operation),
		pendingOperations:   make([]Operation, 0),
		completedOperations: make(map[int][]Operation, 0),
		waitingSites:        make(map[int]bool),
		state:               TxActive,
	}
	t.TransactionGraph.AddNode(tx)
	return nil
}

func (t *TransactionManagerImpl) End(tx int, time int) (CommitResult, error) {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return CommitResult{Wait, ""}, err
	}
	if waiting {
		transaction.appendWaitingOperation(Operation{End, 0, 0, time})
		return CommitResult{Waiting, ""}, nil
	}
	if transaction.state != TxActive {
		return CommitResult{Aborted, "Transaction is not active"}, nil
	}
	for site, operation := range transaction.siteWrites {
		result := t.SiteCoordinator.VerifySiteWrite(site, operation.key, operation.time, time)
		switch result {
		case SiteDown:
			t.abortTransaction(tx)
			return CommitResult{Abort, fmt.Sprintf("Site %d was down between write to x%d and commit", site, operation.key)}, nil
		case SiteStale:
			t.abortTransaction(tx)
			return CommitResult{Abort, fmt.Sprintf("Write to x%d was stale at site %d", operation.key, site)}, nil
		case SiteOk:
			continue
		}
	}
	rwCycles := t.TransactionGraph.FindRWCycles(tx)
	if rwCycles {
		t.abortTransaction(tx)
		return CommitResult{Abort, fmt.Sprintf("Tx: %d, RW cycle detected", tx)}, nil
	}
	// Check for RW cycles in Transaction Graph
	// Commit all values in transaction and remove
	err = t.commitTransaction(tx, time)
	if err != nil {
		return CommitResult{Abort, err.Error()}, nil
	}
	return CommitResult{Success, ""}, nil
}

func (t *TransactionManagerImpl) Write(tx int, key int, value int, time int) (WriteResult, error) {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return WriteResult{Abort, []int{}}, err
	}
	if waiting {
		transaction.appendWaitingOperation(Operation{Write, key, value, time})
		return WriteResult{Waiting, []int{}}, nil
	}
	if transaction.state == TxAborted {
		return WriteResult{Aborted, []int{}}, nil
	}
	writeSites := t.SiteCoordinator.GetActiveSitesForKey(key)
	if len(writeSites) == 0 {
		possibleWriteSites := t.SiteCoordinator.GetSitesForKey(key)
		t.waitTransaction(tx, possibleWriteSites)
		// Check if this was already pending operation
		if len(transaction.pendingOperations) == 0 {
			transaction.appendWaitingOperation(Operation{Write, key, value, time})
		}
		return WriteResult{Wait, writeSites}, nil
	}
	for _, site := range writeSites {
		transaction.addSiteWrite(site, key, value, time)
	}
	t.completeOperation(*transaction, Operation{Write, key, value, time})
	return WriteResult{Success, writeSites}, nil
}

func (t *TransactionManagerImpl) Read(tx int, key int, time int) (ReadResult, error) {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return ReadResult{-1, Abort}, err
	}
	if waiting {
		transaction.appendWaitingOperation(Operation{Read, key, 0, time})
		return ReadResult{-1, Waiting}, nil
	}
	if transaction.state == TxAborted {
		return ReadResult{-1, Aborted}, nil
	}
	transactionStart := transaction.startTime
	siteList := t.SiteCoordinator.GetValidSitesForRead(key, transactionStart)
	if len(siteList) == 0 {
		t.abortTransaction(tx)
		return ReadResult{-1, Abort}, nil
	}
	for _, site := range siteList {
		value, err := t.SiteCoordinator.ReadActiveSite(site, key, transactionStart)
		if err == nil {
			t.completeOperation(*transaction, Operation{Read, key, value.value, time})
			return ReadResult{value.value, Success}, nil
		}
	}
	// Wait transaction logic
	err = t.waitTransaction(tx, siteList)
	// Check if this was already pending operation
	if len(transaction.pendingOperations) == 0 {
		transaction.appendWaitingOperation(Operation{Read, key, 0, time})
	}
	return ReadResult{-1, Wait}, err
}

func (t *TransactionManagerImpl) Recover(site int, time int) error {
	for tx := range t.WaitingTransactions {
		transaction, waiting, err := t.GetTransaction(tx)
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

func (t *TransactionManagerImpl) GetTransaction(tx int) (*Transaction, bool, error) {
	transaction, exists := t.TransactionMap[tx]
	if !exists {
		return &Transaction{}, false, fmt.Errorf("Transaction %d does not exist", tx)
	}
	_, waiting := t.WaitingTransactions[tx]
	return transaction, waiting, nil
}

func (t *TransactionManagerImpl) commitTransaction(tx int, currentTime int) error {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return err
	}
	if waiting {
		return fmt.Errorf("Transaction %d is waiting", tx)
	}
	if transaction.state != TxActive {
		return fmt.Errorf("Transaction %d is not active", tx)
	}
	for site, operation := range transaction.siteWrites {
		err := t.SiteCoordinator.CommitSiteWrite(site, operation.key, operation.value, currentTime)
		if err != nil {
			return err
		}
	}
	transaction.state = TxCommitted
	t.removeTransaction(tx)
	return nil
}

func (t *TransactionManagerImpl) abortTransaction(tx int) error {
	transaction, _, err := t.GetTransaction(tx)
	if err != nil {
		return err
	}
	if transaction.state != TxActive {
		return fmt.Errorf("Transaction %d is not active", tx)
	}
	transaction.state = TxAborted
	t.removeTransaction(tx)
	return nil
}

func (t *TransactionManagerImpl) waitTransaction(tx int, sites []int) error {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return err
	}
	if waiting {
		return nil
	}
	if transaction.state != TxActive {
		return fmt.Errorf("Transaction %d is not active", tx)
	}
	for _, site := range sites {
		transaction.waitingSites[site] = true
	}
	transaction.state = TxWaiting
	t.WaitingTransactions[tx] = true
	return nil
}

func (t *TransactionManagerImpl) unwaitTransaction(tx int) error {
	transaction, waiting, err := t.GetTransaction(tx)
	if err != nil {
		return err
	}
	if !waiting {
		return fmt.Errorf("Transaction %d is not waiting", tx)
	}
	if transaction.state != TxWaiting {
		return fmt.Errorf("Transaction %d is not waiting", tx)
	}
	transaction.waitingSites = make(map[int]bool)
	transaction.state = TxActive
	delete(t.WaitingTransactions, tx)
	return nil
}

func (t *TransactionManagerImpl) runPendingOperations(tx *Transaction, recoverTime int) error {
	for index, operation := range tx.pendingOperations {
		switch operation.operationType {
		case Write:
			result, err := t.Write(tx.id, operation.key, operation.value, recoverTime)
			if err != nil {
				return err
			}
			HandleWriteResult(tx.id, operation.key, result)
			if result.ResultType != Success {
				tx.truncatePendingOperations(index) //Wait or Abort
				return nil
			}
		case Read:
			value, err := t.Read(tx.id, operation.key, recoverTime)
			if err != nil {
				return err
			}
			HandleReadResult(tx.id, operation.key, value)
			if value.ResultType != Success {
				tx.truncatePendingOperations(index) //Wait or Abort
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

func (t *TransactionManagerImpl) removeTransaction(tx int) {
	delete(t.WaitingTransactions, tx)
	t.TransactionGraph.RemoveNode(tx)
}

func (t *TransactionManagerImpl) completeOperation(transaction Transaction, operation Operation) error {
	transaction.appendCompletedOperation(operation)
	conflicts := t.findConflicts(transaction.id, operation)
	for tx, conflictMap := range conflicts {
		for conflictType := range conflictMap {
			t.TransactionGraph.AddEdge(tx, transaction.id, conflictType)
		}
	}
	return nil
}

func (t *TransactionManagerImpl) findConflicts(original int, operation Operation) map[int]map[ConflictType]bool {
	result := make(map[int]map[ConflictType]bool)
	for tx, transaction := range t.TransactionMap {
		if tx != original {
			result[tx] = make(map[ConflictType]bool)
			pastOperations := transaction.completedOperations[operation.key]
			for _, pastOp := range pastOperations {
				switch operation.operationType {
				case Write:
					switch pastOp.operationType {
					case Write:
						result[tx][WW] = true
					case Read:
						result[tx][RW] = true
					}
				case Read:
					switch pastOp.operationType {
					case Write:
						result[tx][WR] = true
					}
				}
			}
		}
	}
	return result
}

/**********
Transaction methods
**********/

func (tx *Transaction) GetState() TransactionState {
	return tx.state
}

func (tx *Transaction) GetSiteWrites() map[int]Operation {
	return tx.siteWrites
}

func (tx *Transaction) appendWaitingOperation(operation Operation) error {
	tx.pendingOperations = append(tx.pendingOperations, operation)
	return nil
}

func (tx *Transaction) appendCompletedOperation(operation Operation) error {
	tx.completedOperations[operation.key] = append(tx.completedOperations[operation.key], operation)
	return nil
}

func (tx *Transaction) addSiteWrite(site int, key int, value int, time int) error {
	tx.siteWrites[site] = Operation{Write, key, value, time}
	return nil
}

func (tx *Transaction) truncatePendingOperations(index int) {
	tx.pendingOperations = tx.pendingOperations[index:]
}

func HandleReadResult(tx int, key int, result ReadResult) {
	switch result.ResultType {
	case Success:
		utils.LogRead(tx, key, result.Value)
	case Abort:
		utils.LogAbort(tx, "")
	case Wait:
		utils.LogWait(tx)
	case Waiting:
		utils.LogWaiting(tx)
	case Aborted:
		utils.LogAborted(tx)
	}
}

func HandleWriteResult(tx int, key int, result WriteResult) {
	switch result.ResultType {
	case Success:
		utils.LogWrite(tx, key, result.Sites)
	case Abort:
		utils.LogAbort(tx, "")
	case Wait:
		utils.LogWait(tx)
	case Waiting:
		utils.LogWaiting(tx)
	case Aborted:
		utils.LogAborted(tx)
	}
}

func HandleCommitResult(tx int, result CommitResult) {
	switch result.ResultType {
	case Success:
		utils.LogCommit(tx)
	case Abort:
		utils.LogAbort(tx, result.reason)
	case Wait:
		utils.LogWait(tx)
	case Waiting:
		utils.LogWaiting(tx)
	case Aborted:
		utils.LogAborted(tx)
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
