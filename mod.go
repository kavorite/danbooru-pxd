package main

import (
	"context"
)

const projectID = "danbooru-px"

func main() {
	var g *BiGraph
	g.LoadPostGraph(context.Background()).FCk()
}
