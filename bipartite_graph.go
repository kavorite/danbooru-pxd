package main

import (
	"context"
	"compress/gzip"
	"encoding/json"
	"bufio"
	"time"
	"io"
	"sync/atomic"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type IDType uint8
type IDValue int64
type TagType uint8

const (
	idTypeTag = 0
	idTypePost = iota
)

type ID struct {
	IDValue
	IDType
}

type Node interface {
	ID() ID
	Neighbors() []Node
}

type Tag struct {
	IDValue			`json:"id,string"`
	TagType			`json:"category,string"`
	Name string
}

func (tag *Tag) ID() ID {
	return ID{tag.IDValue, idTypeTag}
}

type Post struct {
	IDValue		`json:"id,string"`
	Tags []Tag
}

func (post *Post) ID() ID {
	return ID{post.IDValue, idTypePost}
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
