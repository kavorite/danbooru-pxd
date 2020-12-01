package main

import (
	"context"
	"math/rand"
	"time"
	"math"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"gonum.org/v1/gonum/graph/network"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type IDType uint8

const (
	idTypeTag IDType = 0
	idTypePost IDType = iota
)

type IDValue int

type ID struct {
	IDValue
	IDType
}

func (q *ID) PutInt64(n int64) {
	q.IDValue = IDValue(n >> 8)
	q.IDType = IDType(n & 0xff)
}

func (q ID) ToInt64() (n int64) {
	n |= int64(q.IDValue << 8)
	n |= int64(q.IDType)
	return
}

type TagType int

type Node interface {
	ID() ID
	Neighbors() []Node
}

type Tag struct {
	id IDValue
	Name string
	TagType
}

func (tag *Tag) ID() ID {
	return ID{tag.id, idTypeTag}
}

type Post struct {
	id IDValue
	Tags []Tag
}

func (post *Post) ID() ID {
	return ID{post.id, idTypePost}
}

type TagNode struct {
	Tag
	neighbors map[ID]*PostNode
}

func (tn TagNode) Neighbors() (neighbors []Node) {
	neighbors = make([]Node, 0, len(tn.neighbors))
	for _, postNode := range tn.neighbors {
		neighbors = append(neighbors, postNode)
	}
	return
}

type PostNode struct {
	Post
	neighbors map[ID]*TagNode
}

func (post PostNode) Neighbors() (neighbors []Node) {
	neighbors = make([]Node, 0, len(post.neighbors))
	for _, tagNode := range post.neighbors {
		neighbors = append(neighbors, tagNode)
	}
	return
}

type BiGraph map[ID]Node

func (bigraph BiGraph) TagEdges(id IDValue) (*TagNode, bool) {
	node, ok := bigraph[ID{id, idTypeTag}]
	if !ok {
		return nil, false
	}
	return node.(*TagNode), true
}

func (bigraph BiGraph) PostEdges(id IDValue) (*PostNode, bool) {
	node, ok := bigraph[ID{id, idTypePost}]
	if !ok {
		return nil, false
	}
	return node.(*PostNode), true
}

func (bigraph BiGraph) NodeDegree(q ID) int {
	node, ok := bigraph[q]
	if !ok {
		return 0
	}
	return len(node.Neighbors())
}

func (g BiGraph) TagPost(post Post) (degree int) {
	if _, ok := g[post.ID()]; !ok {
		postNode := &PostNode{Post: post}
		postNode.neighbors = make(map[ID]*TagNode, 128)
		g[postNode.ID()] = postNode
	}
	postNode := g[post.ID()].(*PostNode)
	for _, tag := range post.Tags {
		if _, ok := g[tag.ID()].(*TagNode); !ok {
			neighbors := make(map[ID]*PostNode, 1<<20)
			g[tag.ID()] = &TagNode{tag, neighbors}
		}
		g[tag.ID()].(*TagNode).neighbors[postNode.ID()] = postNode
		g[postNode.ID()].(*PostNode).neighbors[tag.ID()] = g[tag.ID()].(*TagNode)
	}
	degree = len(post.Tags)
	return
}

func (bigraph *BiGraph) LoadPostTags(ctx context.Context) *Error {
	client, err := bq.NewClient(ctx, projectID)
	if err := wrapError("init BigQuery client", err); err != nil {
		return err
	}
	var g BiGraph
	if bigraph == nil {
		q := client.Query("SELECT COUNT(id) from `danbooru-px.danbooru_post_tags.taggings`")
		it, err := q.Read(ctx)
		if err := wrapError("count posts", err); err != nil {
			return err
		}
		countRow := make([]bq.Value, 1)
		if err := wrapError("count posts", it.Next(&countRow)); err != nil {
			return err
		}
		postc := countRow[0].(int64)
		g = make(BiGraph, postc)
		bigraph = &g
	}
	g = *bigraph
	q := client.Query("SELECT * FROM `danbooru-px.danbooru_post_tags.taggings`")
	it, err := q.Read(ctx)
	if err != nil && err != iterator.Done {
		return wrapError("hydrate post labels", err)
	}
	for {
		var post Post
		if err := it.Next(&post); err != nil {
			if err != iterator.Done {
				return wrapError("hydrate post labels", err)
			}
			return nil
		}
		bigraph.TagPost(post)
	}
}

type PxGraph struct {
	BiGraph
	MaxPostDegree int
}

func (g PxGraph) TagPost(post Post) {
	if d := g.BiGraph.TagPost(post); d > g.MaxPostDegree {
		g.MaxPostDegree = d
	}
}

type PxQuery struct {
	Ratings map[ID]float64
	MaxVisitsPerWalk int
	MaxTotalVisits int
	centrality map[ID]float64
}

func (px *PxQuery) computeCentrality(g PxGraph) (centrality map[ID]float64) {
	// compute the betweenness centrality of each tag amongst the query points to derive its rating
	subset := make(BiGraph, len(px.Ratings))
	for q := range px.Ratings {
		node := g.BiGraph[q]
		subset[q] = node
	}
	betweenness := network.Betweenness(&BiGraphX{subset})
	centrality = make(map[ID]float64, len(betweenness))
	maxCentrality := 0.0
	var q ID
	for id, score := range betweenness {
		q.PutInt64(id)
		centrality[q] = score
		maxCentrality = math.Max(maxCentrality, score)
	}
	for q := range centrality {
		centrality[q] /= maxCentrality
	}
	return
}

func (px *PxQuery) PersonalizedNeighbor(q ID, g PxGraph) Node {
	neighbors := g.BiGraph[q].Neighbors()
	if px.centrality == nil {
		px.centrality = px.computeCentrality(g)
	}
	rand.Shuffle(len(neighbors), func(i, j int) {
		neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
	})
	goal := rand.Float64() * float64(len(neighbors))
	i := 0
	for {
		node := neighbors[i]
		goal -= (px.centrality[node.ID()] + 1.0) * px.Ratings[q]
		if goal < 0 {
			return node
		}
		i++
		if i == len(neighbors) {
			i = 0
		}
	}
}

func (px *PxQuery) QueryScalingFactor(q ID, g PxGraph) (weight float64) {
	c := math.Abs(float64(g.MaxPostDegree))
	n := math.Abs(float64(len(g.BiGraph[q].Neighbors())))
	weight = n * (c - math.Log(n))
	return
}

func (px *PxQuery) SampleWalkLength(alpha float64) (stepc float64) {
	n := math.Abs(float64(px.MaxTotalVisits))
	nQ := math.Abs(float64(px.MaxVisitsPerWalk))
	if alpha <= 0 || alpha > 1 {
		alpha = nQ / n
	}
	stepc = alpha * (rand.Float64() + 0.5) * n
	return
}

func (px *PxQuery) SampleWalkLengths(g PxGraph) (stepCounts map[ID]float64) {
	stepCounts = make(map[ID]float64, len(px.Ratings))
	for q, rating := range px.Ratings {
		alpha := rating * px.QueryScalingFactor(q, g)
		stepCounts[q] = px.SampleWalkLength(alpha)
	}
	sigma := 0.0
	for _, stepCount := range stepCounts {
		sigma += stepCount
	}
	for q := range stepCounts {
		stepCounts[q] /= sigma
	}
	return
}

// TODO: early stopping
func (px *PxQuery) RunAgainst(g PxGraph, postsOnly bool) (recommendations map[ID]float64) {
	defer func() {
		px.centrality = nil
	}()
	maxPostDegree := float64(g.MaxPostDegree)
	expectedResults := math.Ceil(math.Log(maxPostDegree))
	recommendations = make(map[ID]float64, len(px.Ratings))
	for q, stepc := range px.SampleWalkLengths(g) {
		visits := make(map[ID]float64, int(expectedResults * 1.5))
		for i := 0; i < int(stepc); i++ {
			q = px.PersonalizedNeighbor(q, g).ID()
			if (postsOnly && q.IDType != idTypePost) {
			 	q = px.PersonalizedNeighbor(q, g).ID()
			}
			if _, ok := visits[q]; !ok {
				visits[q] = 0
			}
			visits[q]++
		}
		for q, k := range visits {
			if _, ok := recommendations[q]; !ok {
				recommendations[q] = 0
			}
			recommendations[q] += math.Sqrt(k)
		}
	}
	for q := range recommendations {
		recommendations[q] *= recommendations[q]
	}
	return
}