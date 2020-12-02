package main

import (
	"context"
	"compress/gzip"
	"encoding/json"
	"bufio"
	"time"
	"io"
	"sync/atomic"
	"math/rand"
	"math"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"gonum.org/v1/gonum/graph/network"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Node interface {
	ID() ID
	Neighbors() []Node
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
	gcs, err := storage.NewClient(ctx)
	if err := wrapError("init GCS client", err); err != nil {
		return err
	}
	// check when the tags were last modified
	bkt := gcs.Bucket("danbooru-px-tags")
	attr, err := bkt.Objects(ctx, &storage.Query{Prefix: "tags-"}).Next()
	if err := wrapError("check data freshness", err); err != nil {
		return err
	}
	var extraction *bq.Job
	queryGracePeriod := time.Hour // BigQuery needs time to update our tables
	// TODO: subscribe to pub/sub notifications to know when to do the extraction task
	if time.Now().Sub(attr.Updated) >= time.Hour * 24 + queryGracePeriod {
		gcsRef := bq.NewGCSReference("gs://danbooru-px-tags/tags-*")
		gcsRef.Compression = bq.Gzip
		gcsRef.DestinationFormat = bq.JSON
		extractor := client.Dataset("danbooru_post_tags").Table("taggings").ExtractorTo(gcsRef)
		extractor.DisableHeader = true
		var err error
		extraction, err = extractor.Run(ctx)
		if err := wrapError("extract table data", err); err != nil {
			return err
		}
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
	// hurry up and wait for extraction to complete
	if extraction != nil {
		for {
			<-time.After(time.Second*10)
			status, err := extraction.Status(ctx)
			if err := wrapError("extract table data: poll job status", err); err != nil {
				return err
			}
			if status.State != bq.Done {
				continue
			}
			if len(status.Errors) != 0 {
				return wrapError("extract table data", status.Errors[len(status.Errors)-1])
			}
			break
		}
	}
	// pull our archives out of storage
	posts := make(chan Post)
	errs := make(chan *Error, 1)
	hydrating := int32(0)
	for it := bkt.Objects(ctx, &storage.Query{Prefix: "tags-"});; {
		attr, err := it.Next()
		if err == iterator.Done {
			break
		}
		atomic.AddInt32(&hydrating, 1)
		go func() {
			if err := wrapError("resolve exported JSON blob handle", err); err != nil {
				errs <- err
				return
			}
			blob := bkt.Object(attr.Name)
			var istrm io.ReadCloser
			istrm, err = blob.NewReader(ctx)
			if err := wrapError("hydrate exported data", err); err != nil {
				errs <- err
				return
			}
			defer istrm.Close()
			istrm, err = gzip.NewReader(istrm)
			if err := wrapError("hydrate exported data", err); err != nil {
				errs <- err
				return
			}
			for sc := bufio.NewScanner(istrm); sc.Scan(); {
				post := Post{}
				err := json.Unmarshal(sc.Bytes(), &post)
				if err := wrapError("hydrate exported data", err); err != nil {
					errs <- err
					return
				}
				posts <- post
			}
			if atomic.AddInt32(&hydrating, -1) == 0 {
				close(posts)
			}
		}()
	}
	select {
	case err := <-errs:
		return err
	default:
		break
	}
	for post := range posts {
		select {
		case err := <-errs:
			return err
		default:
			break
		}
		g.TagPost(post)
	}
	return nil
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
	ReturnPosts bool
	ReturnTags bool
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
func (px *PxQuery) RunAgainst(g PxGraph) (recommendations map[ID]float64) {
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
			if (!px.ReturnTags && q.IDType != idTypePost) {
			 	q = px.PersonalizedNeighbor(q, g).ID()
			}
			if (!px.ReturnPosts && q.IDType != idTypeTag) {
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