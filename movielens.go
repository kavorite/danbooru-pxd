package main

import (
	"archive/zip"
	"fmt"
	"bytes"
	"io"
	"net/http"
	"strings"
	"encoding/csv"
	"strconv"
)

type MovieLens struct {
	*zip.Reader
	taggings, ratings *zip.File
}

type (
	MLUserID int
	MLFilmID int
	MLFilmRating int8
	MLRatingsByFilm map[MLFilmID]MLFilmRating
	MLUserRatingsByFilm map[MLUserID]MLRatingsByFilm
)

func (movieLens *MovieLens) Retrieve(dataset string) *Error {
	endpoint := fmt.Sprintf("https://files.grouplens.org/datasets/movielens/%s.zip", dataset)
	rsp, err := http.Get(endpoint)
	if err := wrapError("retrieve latest MovieLens-small", err); err != nil {
		return err
	}
	buf := bytes.Buffer{}
	if rsp.ContentLength > 0 {
		buf.Grow(int(rsp.ContentLength))
	}
	_, err = io.Copy(&buf, rsp.Body)
	if err := wrapError("retrieve latest MovieLens-small", err); err != nil {
		return err
	}
	body := buf.Bytes()
	archive, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err := wrapError("unzip MovieLens archive", err); err != nil {
		return err
	}
	movieLens.Reader = archive
	for _, file := range movieLens.File {
		if strings.HasSuffix(file.Name, "tags.csv") {
			movieLens.taggings = file
			continue
		}
		if strings.HasSuffix(file.Name, "ratings.csv") {
			movieLens.ratings = file
			continue
		}
	}
	if movieLens.taggings == nil {
		return wrapError("unzip MovieLens archive", fmt.Errorf("tags.csv not found"))
	}
	if movieLens.ratings == nil {
		return wrapError("unzip MovieLens archive", fmt.Errorf("ratings.csv not found"))
	}
	return nil
}

func (movieLens MovieLens) UnarchiveTaggings() (*PxGraph, *Error) {
	istrm, err := movieLens.taggings.Open()
	operation := "unarchive MovieLens taggings"
	if err := wrapError(operation, err); err != nil {
		return nil, err
	}
	taggings := csv.NewReader(istrm)
	filmtags := PxGraph{BiGraph: make(BiGraph, 2<<10)}
	tagIds := make(map[string]IDValue, 2<<10)
	// discard header: userId, movieId, tag
	if _, err := taggings.Read(); err != nil && err != io.EOF {
		return nil, wrapError(operation, err)
	}
	for {
		tagging, err := taggings.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err := wrapError(operation, err); err != nil {
				return nil, err
			}
		}
		movieIdStr, tag := tagging[0], tagging[1]
		movieIdInt, err := strconv.ParseInt(movieIdStr, 10, 64)
		operation = fmt.Sprintf("unarchive MovieLens taggings: parse movie ID `%d`", movieIdStr)
		if err := wrapError(operation, err); err != nil {
			return nil, err
		}
		movieId := ID{IDValue(movieIdInt), idTypeAtom}
		if _, ok := tagIds[tag]; !ok {
			tagIds[tag] = IDValue(len(tagIds))
		}
		tagId := ID{tagIds[tag], idTypeSilo}
		filmtags.AddEdge(movieId, tagId)
	}
	return &filmtags, nil
}

func (movieLens MovieLens) UnarchiveRatings() (MLUserRatingsByFilm, *Error) {
	istrm, err := movieLens.ratings.Open()
	operation := "unarchive MovieLens ratings"
	if err := wrapError(operation, err); err != nil {
		return nil, err
	}
	ratingsByUID := make(MLUserRatingsByFilm, 1<<10)
	ratingRows := csv.NewReader(istrm)
	// discard header: userId, movieId, rating
	if _, err := ratingRows.Read(); err != nil && err != io.EOF {
		return nil, wrapError(operation, err)
	}
	for {
		record, err := ratingRows.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err := wrapError(operation, err); err != nil {
				return nil, err
			}
		}
		parseTargets := [3]string{"user ID", "film ID", "rating"}
		parseResults := [3]int64{-1, -1, 0}
		for i := range parseTargets {
			cellValue := record[i]
			target := parseTargets[i]
			parseOp := fmt.Sprintf("%s: parse %s `%s`", operation, target, cellValue)
			parseResult, err := strconv.ParseInt(cellValue, 10, 64)
			if err := wrapError(parseOp, err); err != nil {
				return nil, err
			}
			parseResults[i] = parseResult
		}
		userID := MLUserID(parseResults[0])
		filmID := MLFilmID(parseResults[1])
		rating := MLFilmRating(parseResults[2])
		if _, ok := ratingsByUID[userID]; !ok {
			ratingsByUID[userID] = make(MLRatingsByFilm, 32)
		}
		ratingsByUID[userID][filmID] = rating
	}
	return ratingsByUID, nil
}