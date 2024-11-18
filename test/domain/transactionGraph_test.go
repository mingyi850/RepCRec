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
		graph.AddEdge(1, 2, domain.WEdge)
		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.WEdge, outgoingEdges[2])
	})

	t.Run("Add edge should add edge to graph if does not exist", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		err := graph.AddEdge(1, 3, domain.WEdge)
		assert.NotNil(t, err)
		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, 0, len(outgoingEdges))
	})

	t.Run("Add edge should promde edge to RW if R exists and W is written or vice versa", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.WEdge)
		graph.AddEdge(1, 2, domain.REdge)

		graph.AddEdge(1, 3, domain.REdge)
		graph.AddEdge(1, 3, domain.WEdge)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.RWEdge, outgoingEdges[2])
		assert.Equal(t, domain.RWEdge, outgoingEdges[3])
	})

	t.Run("Add edge should do nothing if edge is R or W and another edge is the same", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.WEdge)
		graph.AddEdge(1, 2, domain.WEdge)

		graph.AddEdge(1, 3, domain.REdge)
		graph.AddEdge(1, 3, domain.REdge)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.WEdge, outgoingEdges[2])
		assert.Equal(t, domain.REdge, outgoingEdges[3])
	})

	t.Run("Add edge should do nothing if edge is RW and another edge is added", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddEdge(1, 2, domain.RWEdge)
		graph.AddEdge(1, 2, domain.WEdge)

		graph.AddEdge(1, 3, domain.RWEdge)
		graph.AddEdge(1, 3, domain.RWEdge)

		outgoingEdges := graph.GetEdges(1)
		assert.Equal(t, domain.RWEdge, outgoingEdges[2])
		assert.Equal(t, domain.RWEdge, outgoingEdges[3])
	})

	t.Run("Should check for RW cycles if exists", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		err := graph.AddEdge(1, 2, domain.RWEdge)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 1, domain.RWEdge)
		assert.Nil(t, err)

		assert.Equal(t, true, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if does not exist", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		err := graph.AddEdge(1, 2, domain.RWEdge)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 3, domain.RWEdge)
		assert.Nil(t, err)
		err = graph.AddEdge(1, 3, domain.RWEdge)
		assert.Nil(t, err)
		assert.Equal(t, false, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if cycles are between last and first elem", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		err := graph.AddEdge(1, 2, domain.RWEdge)
		assert.Nil(t, err)
		err = graph.AddEdge(2, 3, domain.WEdge)
		assert.Nil(t, err)
		err = graph.AddEdge(3, 1, domain.RWEdge)
		assert.Nil(t, err)
		assert.Equal(t, true, graph.FindRWCycles(1))
	})

	t.Run("Should check for RW cycles if cycles are not RW", func(t *testing.T) {
		graph := domain.CreateTransactionGraph()
		graph.AddNode(1)
		graph.AddNode(2)
		graph.AddNode(3)
		graph.AddNode(4)
		graph.AddEdge(1, 2, domain.RWEdge)
		graph.AddEdge(2, 3, domain.WEdge)
		graph.AddEdge(3, 4, domain.RWEdge)
		graph.AddEdge(4, 1, domain.WEdge)

		assert.Equal(t, false, graph.FindRWCycles(1))
	})
}
