/**************************
File: transasctionGraph.go
Author: Mingyi Lim
Description: This file contains the implementation of the TransactionGraph struct. The TransactionGraph is responsible for managing the transaction graph and checking for RW cycles.
***************************/

package domain

import (
	"fmt"
)

/*
************
Custom Structs
************
*/
type Edge struct {
	from     int
	to       int
	edgeType ConflictType
}

/* Uses an adjacency list to represent the transaction graph */
type TransactionGraph struct {
	commitTimes map[int]int
	graph       map[int]map[int]ConflictType
}

/* Creates and returns an instance of the TransactionGraph */
func CreateTransactionGraph() TransactionGraph {
	return TransactionGraph{
		graph:       make(map[int]map[int]ConflictType),
		commitTimes: make(map[int]int),
	}
}

/* Adds a new node to the graph which represents a new transaction */
func (t *TransactionGraph) AddNode(tx int, time int) {
	if _, exists := t.graph[tx]; !exists {
		t.graph[tx] = make(map[int]ConflictType)
		t.commitTimes[tx] = time
	}
}

/*
Adds and edge between two transactions in the graph
If an edge already exists, checks if the new edge is RW and promotes the edge to RW if it is (since those are the only edges of concern)
*/
func (t *TransactionGraph) AddEdge(from int, to int, edgeType ConflictType) error {
	if _, exists := t.graph[from]; !exists {
		return fmt.Errorf("Transaction %d does not exist in Graph", from)
	}
	if _, exists := t.graph[to]; !exists {
		return fmt.Errorf("Transaction %d does not exist in Graph", from)
	}
	if _, exists := t.graph[from][to]; !exists {
		t.graph[from][to] = edgeType
		return nil
	}
	if edgeType == RW { //Promote non-RW edge to RW edge iff new edge is RW
		t.graph[from][to] = RW
	}
	return nil
}

/* Removes a node from the graph, along with all edges from other nodes which include the node */
func (t *TransactionGraph) RemoveNode(tx int) {
	delete(t.graph, tx)
	for _, edges := range t.graph {
		delete(edges, tx)
	}
	delete(t.commitTimes, tx)
}

func (t *TransactionGraph) PurgeGraph(earliestStart int) {
	for tx, time := range t.commitTimes {
		if time < earliestStart {
			t.RemoveNode(tx)
		}
	}
}

func (t *TransactionGraph) TryCommitTransaction(tx int, incomingConflicts map[int]ConflictType, outgoingConflicts map[int]ConflictType, time int) bool {
	t.AddNode(tx, time)
	for from, edgeType := range incomingConflicts {
		t.AddEdge(from, tx, edgeType)
	}
	for to, edgeType := range outgoingConflicts {
		t.AddEdge(tx, to, edgeType)
	}
	if t.FindRWCycles(tx) {
		t.RemoveNode(tx)
		return false
	}
	return true
}

/* Checks if RW-RW cycles exist in the graph. Returns true if so and false otherwise */
func (t *TransactionGraph) FindRWCycles(tx int) bool {
	visited := make(map[int]bool)
	cycles := t.findCycles(tx, tx, visited, make([]Edge, 0))
	for _, cycle := range cycles {
		if t.findConsecutiveRW(cycle) {
			return true
		}
	}
	return false
}

/* Gets map representation of the graph (used for debugging / testing) */
func (t *TransactionGraph) GetGraph() map[int]map[int]ConflictType {
	return t.graph
}

/* Get all edges from a node (for debugging / testing) */
func (t *TransactionGraph) GetEdges(tx int) map[int]ConflictType {
	return t.graph[tx]
}

/* Get all active nodes in the graph */
func (t *TransactionGraph) GetNodes() []int {
	nodes := make([]int, 0)
	for node := range t.graph {
		nodes = append(nodes, node)
	}
	return nodes
}

func (t *TransactionGraph) GetCommitTime(tx int) (int, error) {
	if time, exists := t.commitTimes[tx]; exists {
		return time, nil
	}
	return 0, fmt.Errorf("Transaction %d has not committed", tx)
}

/*
*************************
Private Methods
*************************
*/

/* Finds all cycles in a graph starting from a given node using DFS. Returns a list of paths (which is a list of edges) for inspection */
func (t *TransactionGraph) findCycles(current int, start int, visited map[int]bool, path []Edge) [][]Edge {
	if current == start && len(path) > 1 {
		return append(make([][]Edge, 0), path)
	}
	foundCycles := make([][]Edge, 0)
	if current != start {
		visited[current] = true
	}
	for next, edge := range t.graph[current] {
		if !visited[next] {
			edge := Edge{current, next, edge}
			newPath := append(path, edge)
			foundCycles = append(foundCycles, t.findCycles(next, start, visited, newPath)...)
		}
	}
	visited[current] = false
	return foundCycles
}

/* Checks if a cycle contains consecutive RW edges */
func (t *TransactionGraph) findConsecutiveRW(cycle []Edge) bool {
	prev := false
	if len(cycle) > 2 {
		cycle = append(cycle, cycle[0])
	}
	for _, edge := range cycle {
		isRw := edge.edgeType == RW
		if prev && isRw {
			return true
		}
		prev = isRw
	}
	return false
}
