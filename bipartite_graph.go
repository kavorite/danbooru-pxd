package main

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type IDType int

const (
	idTypeTag = 0
	idTypePost = iota
)

type ID struct {
	Value int
	IDType
}

type TagType int

type Node interface {
	ID() ID
	Neighbors() []Node
}

type Tag struct {
	id int
	Name string
	TagType
}

func (tag *Tag) ID() ID {
	return ID{tag.id, idTypeTag}
}

type Post struct {
	id int
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

func (bigraph BiGraph) TagEdges(id int) (*TagNode, bool) {
	node, ok := bigraph[ID{id, idTypeTag}]
	if !ok {
		return nil, false
	}
	return node.(*TagNode), true
}

func (bigraph BiGraph) PostEdges(id int) (*PostNode, bool) {
	node, ok := bigraph[ID{id, idTypePost}]
	if !ok {
		return nil, false
	}
	return node.(*PostNode), true
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
		postNode := &PostNode{Post: post}
		postNode.neighbors = make(map[ID]*TagNode, 128)
		g[postNode.ID()] = postNode
		for _, tag := range post.Tags {
			if _, ok := g[tag.ID()].(*TagNode); !ok {
				neighbors := make(map[ID]*PostNode, 1<<20)
				g[tag.ID()] = &TagNode{tag, neighbors}
			}
			g[tag.ID()].(*TagNode).neighbors[postNode.ID()] = postNode
			g[postNode.ID()].(*PostNode).neighbors[tag.ID()] = g[tag.ID()].(*TagNode)
		}
	}
}
