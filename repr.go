package main

import "fmt"

type (
	IDType uint8
	IDValue int64
	TagType int8
	Rating int8
)

const (
	idTypeTag = 0
	idTypePost = iota
)

const (
	tagTypeUnspecified = -1
	tagTypeGeneral = 0
	tagTypeArtist = 1
	tagTypeCopyright = 3
	tagTypeCharacter = 4
	tagTypeMeta = 5
)

func (tagType *TagType) UnmarshalJSON(payload []byte) error {
	*tagType = tagTypeUnspecified
	if string(payload) == "null" {
		return nil
	}
	offset := TagType(payload[0]) - TagType('0')
	tagTypes := map[TagType]struct{}{
		tagTypeGeneral: struct{}{},
		tagTypeArtist: struct{}{},
		tagTypeCopyright: struct{}{},
		tagTypeCharacter: struct{}{},
		tagTypeMeta: struct{}{},
	}
	if _, ok := tagTypes[offset]; !ok {
		valid := make([]interface{}, 0, len(tagTypes))
		for tt := range tagTypes {
			valid = append(valid, tt)
		}
		return fmt.Errorf(
			"tag type must be one of general (%d), " +
			" artist (%d), copyright (%d), character (%d), " +
			" or meta (%d)", valid...
		)
	}
	*tagType = offset
	return nil
}

const (
	ratingUnspecified = -1
	ratingSafe = iota
	ratingQuestionable = iota
	ratingExplicit = iota
)

func (rating *Rating) UnmarshalJSON(payload []byte) error {
	*rating = ratingUnspecified
	if string(payload) == "null" {
		return nil
	}
	if len(payload) != 1 {
		return fmt.Errorf("payload width mismatch")
	}
	ratings := map[byte]Rating{
		's': ratingSafe, 'q': ratingQuestionable, 'e': ratingExplicit,
	}
	r, ok := ratings[payload[0]]
	if !ok {
		return fmt.Errorf(
			"invalid content-rating token '%c' " +
			"(must be one of 's', 'q', or 'e')",
			payload[0],
		)
	}
	*rating = r
	return nil
}

type ID struct {
	IDValue
	IDType
}

type Tag struct {
	IDValue `json:"id,string"`
	TagType `json:",string"`
	Name string
}

func (tag *Tag) ID() ID {
	return ID{tag.IDValue, idTypeTag}
}

type Post struct {
	IDValue `json:"id,string"`
	Rating `json:",string"`
	Tags []Tag
}

func (post *Post) ID() ID {
	return ID{post.IDValue, idTypePost}
}
