package main

import (
	"testing"
)

// Process

func TestProcess_stripsNonAlphanumeric(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"hello,", "hello"},
		{"world!", "world"},
		{"foo.bar", "foobar"},
		{"123", "123"},
		{"", ""},
		{"...", ""},
		{"don't", "dont"},
	}
	for _, c := range cases {
		if got := Process(c.in); got != c.want {
			t.Errorf("Process(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Hash

func TestHash_deterministic(t *testing.T) {
	for _, word := range []string{"hello", "world", "foo", ""} {
		if Hash(word) != Hash(word) {
			t.Errorf("Hash(%q) not deterministic", word)
		}
	}
}

func TestHash_differentWords(t *testing.T) {
	if Hash("hello") == Hash("world") {
		t.Error("hash collision between 'hello' and 'world'")
	}
}

// Reduce

func TestReduce_deduplicates(t *testing.T) {
	kv := Reduce("word", []string{"2", "1", "1", "3", "2"})
	if kv.key != "word" {
		t.Errorf("key = %q, want %q", kv.key, "word")
	}
	if kv.value != "1,2,3" {
		t.Errorf("value = %q, want %q", kv.value, "1,2,3")
	}
}

func TestReduce_singleDoc(t *testing.T) {
	kv := Reduce("foo", []string{"0"})
	if kv.value != "0" {
		t.Errorf("value = %q, want %q", kv.value, "0")
	}
}

func TestReduce_alreadySorted(t *testing.T) {
	kv := Reduce("bar", []string{"0", "1", "2"})
	if kv.value != "0,1,2" {
		t.Errorf("value = %q, want %q", kv.value, "0,1,2")
	}
}
