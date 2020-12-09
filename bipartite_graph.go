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
	"unsafe"
	"sync"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"gonum.org/v1/gonum/graph/network"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type EdgeSet map[ID]struct{}

type BiGraph map[ID]EdgeSet

func (g BiGraph) PlotNode(id ID) {
	if _, ok := g[id]; ok {
		return
	}
	g[id] = make(EdgeSet, 128)
}

func (g BiGraph) Degree(id ID) int {
	if _, ok := g[id]; !ok {
		return 0
	}
	return len(g[id]) + 1
}

func (g BiGraph) AddEdge(p, q ID) {
	g.PlotNode(p)
	g.PlotNode(q)
	g[p][q] = struct{}{}
	g[q][p] = struct{}{}
}

func (g BiGraph) Neighbors(id ID) (nn []ID) {
	nn = make([]ID, 0, len(g[id]))
	for nid := range g[id] {
		nn = append(nn, nid)
	}
	return
}

type PxGraph struct {
	BiGraph
	sync.RWMutex
	maxDegrees map[IDType]int
}

func (g *PxGraph) PlotNode(id ID) {
	defer g.Unlock()
	g.Lock()
	g.PlotNode(id)
}

func (g *PxGraph) Degree(id ID) int {
	defer g.RUnlock()
	g.RLock()
	return g.BiGraph.Degree(id)
}

func (g *PxGraph) Neighbors(id ID) []ID {
	defer g.RUnlock()
	g.RLock()
	return g.BiGraph.Neighbors(id)
}

func (g *PxGraph) init() {
	g.Lock()
	defer g.Unlock()
	if g.maxDegrees == nil {
		keyspan := int(unsafe.Sizeof(IDValue(0)))
		g.maxDegrees = make(map[IDType]int, keyspan)
	}
}

func (g *PxGraph) AddEdge(p, q ID) {
	g.init()
	g.BiGraph.AddEdge(p, q)
	g.Lock()
	defer g.Unlock()
	for _, id := range []ID{p, q} {
		kind := id.IDType
		if _, ok := g.maxDegrees[kind]; !ok {
			g.maxDegrees[id.IDType] = 1
		}
		if d := g.Degree(p); d > g.maxDegrees[kind] {
			g.maxDegrees[kind] = d
		}
	}
}

func (g *PxGraph) MaxDegree(kind IDType) int {
	g.init()
	g.RLock()
	defer g.RUnlock()
	d, ok := g.maxDegrees[kind]
	if !ok {
		return 0
	}
	return d
}

type PostGraph struct {
	Posts map[ID]Post
	Tags map[ID]Tag
	PxGraph
}

func (postGraph *PostGraph) TagPost(post Post) {
	postID := post.ID()
	for _, tag := range post.Tags {
		tagID := tag.ID()
		postGraph.AddEdge(postID, tagID)
		postGraph.Tags[tagID] = tag
	}
	postGraph.Posts[postID] = post
}

func runCountQuery(q bq.Query, ctx context.Context) (int64, error) {
	it, err := q.Read(ctx)
	if err != nil {
		return -1, err
	}
	countRow := make([]bq.Value, 1)
	if err = it.Next(&countRow); err != nil {
		return -1, err
	}
	return countRow[0].(int64)
}

func (postGraph *PostGraph) LoadPostTags(ctx context.Context) *Error {
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
	if time.Since(attr.Updated) >= time.Hour * 24 + queryGracePeriod {
		gcsRef := bq.NewGCSReference("gs://danbooru-px-tags/tags-*.json.gz")
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
	g := PostGraph{}
	if postGraph == nil {
		q := client.Query("SELECT COUNT(id) from `danbooru-px.danbooru_post_tags.taggings`")
		postc, err := runCountQuery(q)
		if err := wrapError("count posts", err); err != nil {
			return err
		}
		q = client.Query(`
			WITH labels AS (
			  WITH tags AS (
			    SELECT tags FROM ` + "`danbooru-data.danbooru.posts`" + `
			  SELECT DISTINCT name FROM tags tag
			  CROSS JOIN UNNEST(tags))
			SELECT COUNT(name) FROM labels
		`, client, ctx)
		tagc, err := runCountQuery(q, ctx)
		if err := wrapError("count unique tags", err); err != nil {
			return err
		}
		postGraph = &PostGraph{
			Posts: make(map[ID]Post, int(postc)),
			Tags: make(map[ID]Tag, int(tagc)),
			PxGraph: PxGraph{BiGraph: make(BiGraph, int(postc + tagc))},
		}
	}
	g = *postGraph
	// if extraction initiated...
	if extraction != nil {
		// hurry up and wait for GCS extraction to complete
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
	// pull our archives out of GCS
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

type PxQuery struct {
	Ratings map[ID]float64
	ReturnTypes map[IDType]struct{}
	MaxVisitsPerWalk int
	MaxTotalVisits int
}

func (px *PxQuery) WithRatings(ratings map[ID]float64) *PxQuery {
	px.Ratings = ratings
	return px
}

func (px *PxQuery) WithReturnTypes(types ...IDType) *PxQuery {
	px.ReturnTypes = make(map[IDType]struct{}, len(types))
	for _, kind := range types {
		px.ReturnTypes[kind] = struct{}{}
	}
	return px
}

func (px *PxQuery) WithMaxWalkVisits(n int) *PxQuery {
	px.MaxTotalVisits = n
	return px
}

func (px *PxQuery) WithMaxTotalVisits(n int) *PxQuery {
	px.MaxTotalVisits = n
	return px
}

func (px *PxQuery) PersonalizedNeighbor(q ID, g PxGraph) ID {
	neighbors := g.Neighbors(q)
	rand.Shuffle(len(neighbors), func(i, j int) {
		neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
	})
	goal := rand.Float64() * float64(len(neighbors))
	i := 0
	for {
		siblingID := neighbors[i]
		degree := math.Abs(float64(g.Degree(siblingID)))
		maxDegree := math.Abs(float64(g.MaxDegree(siblingID.IDType)))
		degree /= maxDegree
		goal -= (degree + 1.0) * px.Ratings[q]
		if goal < 0 {
			return siblingID
		}
		i++
		if i == len(neighbors) {
			i = 0
		}
	}
}

func (px *PxQuery) QueryScalingFactor(q ID, g PxGraph) (weight float64) {
	c := math.Abs(float64(g.MaxDegree(q.IDType)))
	n := math.Abs(float64(g.Degree(q)))
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
	expectedResults := math.Ceil(math.Log(math.Abs(float64(g.MaxDegree(idTypeAtom)))))
	recommendations = make(map[ID]float64, len(px.Ratings))
	for q, stepc := range px.SampleWalkLengths(g) {
		visits := make(map[ID]float64, int(expectedResults * 1.5))
		for i := 0; i < int(stepc); i++ {
			q = px.PersonalizedNeighbor(q, g)
			for {
				if px.ReturnTypes == nil {
					break
				}
				if _, ok := px.ReturnTypes[q.IDType]; !ok {
					q = px.PersonalizedNeighbor(q, g)
					continue
				}
				break
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
			recommendations[q] += k*k
		}
	}
	for q := range recommendations {
		recommendations[q] = math.Sqrt(recommendations[q])
	}
	return
}