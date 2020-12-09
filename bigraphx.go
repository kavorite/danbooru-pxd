package main

import "gonum.org/v1/gonum/graph"

type IDX struct { id ID }

func (q IDX) ID() int64 {
	return q.id.ToInt64()
}

type BiGraphX struct { BiGraph }

type BiGraphXEdge struct {
	to, from IDX
}

func (edge BiGraphXEdge) From() graph.Node {
	return &edge.from
}

func (edge BiGraphXEdge) To() graph.Node {
	return &edge.to
}

func (edge BiGraphXEdge) ReversedEdge() graph.Edge {
	return &BiGraphXEdge{edge.from, edge.to}
}

type BiGraphXIterator struct {
	BiGraphX
	itinerary []IDX
}

func (it *BiGraphXIterator) Next() bool {
	if (it.itinerary == nil) {
		it.Reset()
	}
	if len(it.itinerary) == 0 {
		return false
	}
	it.itinerary = it.itinerary[1:]
	return len(it.itinerary) > 0
}

func (it *BiGraphXIterator) Len() int {
	return len(it.itinerary)
}

func (it *BiGraphXIterator) Reset() {
	it.itinerary = make([]IDX, 0, len(it.BiGraph))
	for q := range it.BiGraph {
		it.itinerary = append(it.itinerary, IDX{q})
	}
}

func (it *BiGraphXIterator) Node() graph.Node {
	if !it.Next() {
		return nil
	}
	return it.itinerary[0]
}

func (g BiGraphX) Node(id int64) graph.Node {
	q := new(IDX)
	q.id.PutInt64(id)
	return q
}

func (g BiGraphX) Nodes() graph.Nodes {
	return &BiGraphXIterator{BiGraphX: g}
}

func (g BiGraphX) From(n int64) graph.Nodes {
	q := new(IDX)
	q.id.PutInt64(n)
	neighbors := g.Neighbors(q.id)
	subgraph := make(BiGraph, len(neighbors))
	for _, id := range neighbors {
		subgraph[id] = nil
	}
	return BiGraphX{subgraph}.Nodes()
}

func (g BiGraphX) Edge(uid, vid int64) graph.Edge {
	var p, q ID
	p.PutInt64(uid)
	q.PutInt64(vid)
	for _, id := range g.Neighbors(p) {
		if id == q {
			return &BiGraphXEdge{from: IDX{p}, to: IDX{q}}
		}
	}
	return nil
}

func (g BiGraphX) HasEdgeFromTo(pid, qid int64) bool {
	return g.Edge(pid, qid) != nil
}

func (g BiGraphX) HasEdgeBetween(pid, qid int64) bool {
	return g.HasEdgeFromTo(pid, qid) || g.HasEdgeFromTo(qid, pid)
}