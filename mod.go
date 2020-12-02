package main

import (
	"context"
)

const projectID = "danbooru-px"

func main() {
	var g *BiGraph
	g.LoadPostTags(context.Background()).FCk()
}
