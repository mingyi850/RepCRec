package test

import (
	"testing"

	"github.com/mingyi850/repcrec/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestTransactionGraph(t *testing.T) {
	t.Run("AddNode should add nodes to graph", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		assert.Equal(t, len(graph.GetGraph()), 3)
	})

	t.Run("Add edge should add edge to graph if it exists", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddEdge(1, 2, domain.WW)
		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.WW, outgoingEdges[2])
	})

	t.Run("Add edge should return error if node does not exist", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		err := graph.AddEdge(1, 3, domain.WW)
		assert.NotNil(t, err)
		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, 0, len(outgoingEdges))
	})

	t.Run("Add edge should promde edge to RW previous edge is non-RW", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.WW)
		graph.AddEdge(1, 2, domain.RW)

		graph.AddEdge(1, 3, domain.WR)
		graph.AddEdge(1, 3, domain.RW)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.RW, outgoingEdges[2])
		assert.Equal(t, domain.RW, outgoingEdges[3])
	})

	t.Run("Add edge should do nothing if edge is R or W and another edge is the same", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.WW)
		graph.AddEdge(1, 2, domain.WR)

		graph.AddEdge(1, 3, domain.WR)
		graph.AddEdge(1, 3, domain.WW)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.WW, outgoingEdges[2])
		assert.Equal(t, domain.WR, outgoingEdges[3])
	})

	t.Run("Add edge should do nothing if edge is RW and another edge is added", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.RW)
		graph.AddEdge(1, 2, domain.WW)

		graph.AddEdge(1, 3, domain.RW)
		graph.AddEdge(1, 3, domain.WR)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.RW, outgoingEdges[2])
		assert.Equal(t, domain.RW, outgoingEdges[3])
	})

	t.Run("Should check for RW cycles if exists", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		err := graph.AddEdge(1, 2, domain.RW)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 1, domain.RW)
		assert.Nil(t, err)

		assert.Equal(t, true, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if does not exist", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		err := graph.AddEdge(1, 2, domain.RW)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 3, domain.RW)
		assert.Nil(t, err)
		err = graph.AddEdge(1, 3, domain.RW)
		assert.Nil(t, err)
		assert.Equal(t, false, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if cycles are between last and first elem", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		err := graph.AddEdge(1, 2, domain.RW)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 3, domain.WW)
		assert.Nil(t, err)
		err = graph.AddEdge(3, 1, domain.RW)
		assert.Nil(t, err)
		assert.Equal(t, true, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if cycles are not RW", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddNode(4)
		graph.AddEdge(1, 2, domain.RW)
		graph.AddEdge(2, 3, domain.WW)
		graph.AddEdge(3, 4, domain.RW)
		graph.AddEdge(4, 1, domain.WR)

		assert.Equal(t, false, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles and return false if loop is not closed with start", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddNode(4)
		graph.AddEdge(1, 2, domain.RW)
		graph.AddEdge(2, 3, domain.WW)
		graph.AddEdge(3, 4, domain.RW)
		graph.AddEdge(4, 2, domain.RW)
		graph.AddEdge(3, 1, domain.WR)

		assert.Equal(t, false, graph.FindRWCycles(1))
	})
}
