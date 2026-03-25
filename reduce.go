package main

import (
	"slices"
	"strings"
)

func Reduce(key string, values []string) KeyValue {
	slices.Sort(values)
	values = slices.Compact(values)
	var kv KeyValue
	kv.key = key
	kv.value = strings.Join(values, ",")
	return kv
}
