package utility

import "strings"

func CommaStringToSet(s string) (set map[string]struct{}) {
	set = make(map[string]struct{})
	if strings.TrimSpace(s) == "" {
		return
	}
	for _, v := range strings.Split(s, ",") {
		set[strings.TrimSpace(v)] = struct{}{}
	}
	return
}
