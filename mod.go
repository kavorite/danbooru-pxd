package main

import (
	_ "context"
)

const projectID = "danbooru-px"

func main() {
	// g.LoadPostTags(context.Background()).FCk()
	// TODO(kavorite): Evaluate random walk
	// 1. Load MovieLens tags and ratings, split ratings into testing and training sets
	// archive := MovieLens{}
	// wrapError("", archive.Retrieve("ml-latest-small")).FCk()
	// taggings, err := archive.UnarchiveTaggings()
	// wrapError("", err).FCk()
	// ratings, err := archive.UnarchiveRatings()
	// wrapError("", err).FCk()
	// 2. Using Pixie, recommend top-*k* new movies for users by querying the
	//    random-walk function based on the training set

	// 3. Evaluate online recommendations vs ground truth and offline methods:
	//     3a. Count hits (recall): compute the rate at which a movie given an
	//     explicit rating by the user in the training set is retrieved as a
	//     top-*k* recommendation from the test set; further normalize this
	//     metric by dividing by the portion of the data-set that a user has
	//     rated
	//
	//     3b. Compute the rate at which a hit is also given a _positive_
	//     explicit rating by the user (precision), or cross-validate using
	//     root-mean-squared error of ground-truth ratings retrieved against
	//     ratings predicted using [gorse](https://github.com/zhenghaoz/gorse)
	//
	//     3c. Compute an F1 Score (precision * recall / (precision + recall))


}
