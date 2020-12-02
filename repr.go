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
	if len(payload) != 3 {
		return fmt.Errorf("payload width mismatch")
	}
	r := byte(0)
	if _, err := fmt.Sscanf(string(payload), `"%c"`, &r); err != nil {
		return err
	}
	ratings := map[byte]Rating{
		's': ratingSafe, 'q': ratingQuestionable, 'e': ratingExplicit,
	}
	if _, ok := ratings[r]; !ok {
		return fmt.Errorf(
			"invalid content-rating token '%c' " +
			"(must be one of 's', 'q', or 'e')",
			payload[0],
		)
	}
	*rating = Rating(r)
	return nil
}

type ID struct {
	Value int
	IDType
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
	Rating
}

func (post *Post) ID() ID {
	return ID{post.id, idTypePost}
}
