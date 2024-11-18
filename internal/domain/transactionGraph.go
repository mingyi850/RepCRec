package domain

import "fmt"

type Edge struct {
	from     int
	to       int
	edgeType ConflictType
}

type TransactionGraph struct {
	graph map[int]map[int]ConflictType
}

func CreateTransactionGraph() TransactionGraph {
	return TransactionGraph{
		graph: make(map[int]map[int]ConflictType),
	}
}

func (t *TransactionGraph) GetGraph() map[int]map[int]ConflictType {
	return t.graph
}

func (t *TransactionGraph) AddNode(tx int) {
	if _, exists := t.graph[tx]; !exists {
		t.graph[tx] = make(map[int]ConflictType)
	}
}

func (t *TransactionGraph) GetEdges(tx int) map[int]ConflictType {
	return t.graph[tx]
}

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

func (t *TransactionGraph) RemoveNode(tx int) {
	delete(t.graph, tx)
	for _, edges := range t.graph {
		delete(edges, tx)
	}
}

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
