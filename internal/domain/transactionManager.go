/**************************
File: transactionManager.go
Author: Mingyi Lim
Description: This file contains the implementation of the TransactionManager interface. The TransactionManager is responsible for managing the transactions. It provides interfaces to begin, end, write, and read transactions. It also provides the interface to recover transactions.
***************************/

package domain

import (
	"fmt"

	"github.com/mingyi850/repcrec/internal/utils"
)

/*
*********
Consts and Enums
*********
*/
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

type OperationResultType string

const (
	Abort   OperationResultType = "abort"
	Wait    OperationResultType = "wait"
	Success OperationResultType = "success"
	Waiting OperationResultType = "waiting"
	Aborted OperationResultType = "aborted"
)

type TransactionState string

const (
	TxActive    TransactionState = "active"
	TxWaiting   TransactionState = "waiting"
	TxAborted   TransactionState = "aborted"
	TxCommitted TransactionState = "committed"
)

/*
********
Custom Structs
*********
*/

/* Operation represents a single operation in a transaction */
type Operation struct {
	operationType OperationType
	key           int
	value         int
	time          int
}

/* Represents the result of a commit operation. Includes reason if ResultType is Abort */
type CommitResult struct {
	ResultType OperationResultType
	reason     string
}

/* Represents the result of a write operation. Includes sites written to if ResultType is Success */
type WriteResult struct {
	ResultType OperationResultType
	Sites      []int
}

/* Represents the result of a read operation. Includes read value if ResultType is Success */
type ReadResult struct {
	Value      int
	ResultType OperationResultType
}

/*
	Represents a single transaction. We keep track of

1. siteWrites - the last write to each site performed by a transaction (new writes overwrite old writes)
2. pendingOperations - operations that are waiting to be executed
3. completedOperations - all operations that have been completed by the transaction. Key is the key of the operation
4. waitingSites - sites that the transaction is waiting on
5. state - the state of the transaction
*/
type Transaction struct {
	id                  int
	startTime           int
	siteWrites          map[int]Operation
	pendingOperations   []Operation
	completedOperations map[int][]Operation
	waitingSites        map[int]bool
	state               TransactionState
}

/*
	Represents the TransactionManager interface.

The TransactionManager is responsible for managing the transactions.
It provides interfaces to begin, end, write, and read transactions.
It also provides the interface to recover transactions.
*/
type TransactionManager interface {
	Begin(tx int, time int) error
	End(tx int, time int) (CommitResult, error) // Either "commit" or "abort"
	Write(tx int, key int, value int, time int) (WriteResult, error)
	Read(tx int, key int, time int) (ReadResult, error) // Returns read value if available
	Recover(site int, time int) error
	GetTransaction(tx int) (*Transaction, bool, error)
}

/*
	Each Transaction Manager stores

1. SiteCoordinator -> To interact with the sites
2. TransactionMap -> Map of id to a transaction struct
3. WaitingTransactions -> Set of transactions that are waiting
4. TransactionGraph -> Graph of transactions and their conflicts
*/
type TransactionManagerImpl struct {
	SiteCoordinator     SiteCoordinator
	TransactionMap      map[int]*Transaction
	WaitingTransactions map[int]bool
	TransactionGraph    TransactionGraph
}

/* Creates and returns an instance of the TransactionManager */
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
/* Begins a new transaction with the given id and start time - loads the transactionMap and transactionGraph. */
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

/*
	Ends a transaction with the given id and end time - Tries to commit if possible based on

Performs sanity checks on the transaction
Verifies that all writes to sites are valid and not stale
a. If site was down after write to site occured, abort with reason SiteDown
b. If site has been written to since the write to the site, abort with reason SiteStale
Checks for RW cycles in the transaction graph
Commits the transaction if all checks pass
*/
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
	err = t.commitTransaction(tx, time)
	if err != nil {
		return CommitResult{Abort, err.Error()}, nil
	}
	return CommitResult{Success, ""}, nil
}

/* Writes a value to a key at all available sites holding the key. If the key is not available, waits for the key to become available */
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

/*
	Reads a value from a key at all available sites holding the key.

If there are not valid sites to read from, aborts the transaction immediately
If there are valid sites but the site is down, waits for the site to recover
If there are valid sites and the site is up, reads the value from the site
*/
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
	err = t.waitTransaction(tx, siteList)
	// Check if this was already pending operation
	if len(transaction.pendingOperations) == 0 {
		transaction.appendWaitingOperation(Operation{Read, key, 0, time})
	}
	return ReadResult{-1, Wait}, err
}

/*
Recovers a site at the given time.
Looks for transactions which were waiting on the recovered site.
Runs all pending operations in a single time unit on the site for all transactions that were waiting on the site
*/
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

/* Returns the transaction with the given id, a boolean indicating if the transaction is waiting, and an error if the transaction does not exist */
func (t *TransactionManagerImpl) GetTransaction(tx int) (*Transaction, bool, error) {
	transaction, exists := t.TransactionMap[tx]
	if !exists {
		return &Transaction{}, false, fmt.Errorf("Transaction %d does not exist", tx)
	}
	_, waiting := t.WaitingTransactions[tx]
	return transaction, waiting, nil
}

/*
************************************
Private Methods for TransactionManagerImpl
**************************************
*/
/* Commits a transaction by committing all writes to the sites and updating the transaction state. Removes the transaction from the TransactionGraph */
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

/* Aborts a transaction and updates the transaction state. Removes transaction from the TransactionGraph */
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

/* Changes a transaction state to waiting and tracks the sites that the transaction is waiting on */
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

/* Changes a transaction state to active and removes the sites that the transaction was waiting on */
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

/* Runs all pending operations on a transaction. Truncates the pending operations if the operation requires another wait */
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

/* Removes a transaction from the TransactionMap and TransactionGraph */
func (t *TransactionManagerImpl) removeTransaction(tx int) {
	delete(t.WaitingTransactions, tx)
	t.TransactionGraph.RemoveNode(tx)
}

/* Completes an operation by adding the operation to the transaction's completed operations and updating the TransactionGraph with conflicts */
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

/* Finds conflicts between a transaction and all other transactions in the TransactionMap */
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

/* Returns the state of the transaction */
func (tx *Transaction) GetState() TransactionState {
	return tx.state
}

/* Returns the sites a transaction has written to */
func (tx *Transaction) GetSiteWrites() map[int]Operation {
	return tx.siteWrites
}

/*
*************
Private methods
*************
*/
/* Appends an operation to the pending operations of a transaction */
func (tx *Transaction) appendWaitingOperation(operation Operation) error {
	tx.pendingOperations = append(tx.pendingOperations, operation)
	return nil
}

/* Appends a completed operation to the completed operations of a transaction */
func (tx *Transaction) appendCompletedOperation(operation Operation) error {
	tx.completedOperations[operation.key] = append(tx.completedOperations[operation.key], operation)
	return nil
}

/* Adds a write operation to the siteWrites map of a transaction */
func (tx *Transaction) addSiteWrite(site int, key int, value int, time int) error {
	tx.siteWrites[site] = Operation{Write, key, value, time}
	return nil
}

/* Truncates the pending operations of a transaction */
func (tx *Transaction) truncatePendingOperations(index int) {
	tx.pendingOperations = tx.pendingOperations[index:]
}

/*
*************
Utility Functions
*************
*/

/* Handles the printed output of a read operation */
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

/* Handles the printed output of a write operation */
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

/* Handles the printed output of a commit operation */
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
