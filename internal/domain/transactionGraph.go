package domain

import "fmt"

type EdgeType int

const (
	WEdge  EdgeType = 1
	REdge  EdgeType = 2
	RWEdge EdgeType = 3
)

type Edge struct {
	from     int
	to       int
	edgeType EdgeType
}

type TransactionGraph struct {
	graph map[int]map[int]EdgeType
}

func CreateTransactionGraph() TransactionGraph {
	return TransactionGraph{
		graph: make(map[int]map[int]EdgeType),
	}
}

func (t *TransactionGraph) GetGraph() map[int]map[int]EdgeType {
	return t.graph
}

func (t *TransactionGraph) AddNode(tx int) {
	if _, exists := t.graph[tx]; !exists {
		t.graph[tx] = make(map[int]EdgeType)
	}
}

func (t *TransactionGraph) GetEdges(tx int) map[int]EdgeType {
	return t.graph[tx]
}

func (t *TransactionGraph) AddEdge(from int, to int, edgeType EdgeType) error {
	if _, exists := t.graph[from]; !exists {
		return fmt.Errorf("Transaction %d does not exist in Graph", from)
	}
	if _, exists := t.graph[to]; !exists {
		return fmt.Errorf("Transaction %d does not exist in Graph", from)
	}
	if prevEdge, exists := t.graph[from][to]; !exists {
		t.graph[from][to] = edgeType
		return nil
	} else {
		switch prevEdge {
		case WEdge:
			if edgeType == REdge {
				t.graph[from][to] = RWEdge
			}
		case REdge:
			if edgeType == WEdge {
				t.graph[from][to] = RWEdge
			}
		case RWEdge:
			// Do nothing - already RW edge
		}
		return nil
	}
}

func (t *TransactionGraph) RemoveNode(tx int) {
	delete(t.graph, tx)
	for _, edges := range t.graph {
		delete(edges, tx)
	}
}

func (t *TransactionGraph) FindRWCycles(tx int) bool {
	visited := make(map[int]bool)
	cycles := t.findCycle(tx, tx, visited, make([]Edge, 0))
	for _, cycle := range cycles {
		if t.findConsecutiveRW(cycle) {
			return true
		}
	}
	return false
}

func (t *TransactionGraph) findCycle(current int, start int, visited map[int]bool, path []Edge) [][]Edge {
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
			foundCycles = append(foundCycles, t.findCycle(next, start, visited, newPath)...)
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
		isRw := edge.edgeType == RWEdge
		if prev && isRw {
			return true
		}
		prev = isRw
	}
	return false
}
