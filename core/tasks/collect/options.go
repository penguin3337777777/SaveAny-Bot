package collect

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/krau/SaveAny-Bot/common/utils/strutil"
)

type Options struct{ Chat, Tag, Kind, Storage string }

var hashtag = regexp.MustCompile(`#[\p{L}\p{M}\p{N}_]+`)

func Parse(command string) (Options, error) {
	a := strutil.ParseArgsRespectQuotes(command)
	o := Options{Kind: "video"}
	if len(a) < 4 {
		return o, fmt.Errorf("usage")
	}
	o.Chat = a[1]
	seen := make(map[string]bool)
	for i := 2; i < len(a); i += 2 {
		if i+1 == len(a) || seen[a[i]] {
			return o, fmt.Errorf("missing or repeated option")
		}
		seen[a[i]] = true
		switch a[i] {
		case "--tag":
			o.Tag = a[i+1]
		case "--type":
			o.Kind = strings.ToLower(a[i+1])
		case "--storage":
			o.Storage = a[i+1]
		default:
			return o, fmt.Errorf("unknown option")
		}
	}
	if !strings.HasPrefix(o.Tag, "#") {
		o.Tag = "#" + o.Tag
	}
	if hashtag.FindString(o.Tag) != o.Tag {
		return o, fmt.Errorf("invalid hashtag")
	}
	if o.Kind != "video" {
		return o, fmt.Errorf("only video is supported")
	}
	return o, nil
}

func Matches(text, tag string) bool {
	for _, match := range hashtag.FindAllString(text, -1) {
		if strings.EqualFold(match, tag) {
			return true
		}
	}
	return false
}
