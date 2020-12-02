package main

type (
	IDType uint8
	IDValue int64
	TagType int8
	Rating string
)

const (
	idTypeTag = 0
	idTypePost = iota
)

const (
	tagTypeUnspecified TagType = -1
	tagTypeGeneral = 0
	tagTypeArtist = 1
	tagTypeCopyright = 3
	tagTypeCharacter = 4
	tagTypeMeta = 5
)

const (
	ratingSafe Rating = "s"
	ratingQuestionable = "q"
	ratingExplicit = "e"
)

type ID struct {
	IDValue
	IDType
}

type Tag struct {
	IDValue		`json:"id,string"`
	TagType		`json:"category,string"`
	Name string
}

func (tag *Tag) ID() ID {
	return ID{tag.IDValue, idTypeTag}
}

type Post struct {
	IDValue		`json:"id,string"`
	Rating
	Tags []Tag
}

func (post *Post) ID() ID {
	return ID{post.IDValue, idTypePost}
}
