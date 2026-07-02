//go:build integration

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runCompose runs `docker compose -f <file> ...` from the project root.
func runCompose(t *testing.T, composeFile string, args ...string) {
	t.Helper()
	full := append([]string{"compose", "-f", composeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker compose -f %s %s failed: %v", composeFile, strings.Join(args, " "), err)
	}
}

// cleanupCluster tears down containers and volumes.
func cleanupCluster(t *testing.T, composeFile string) {
	t.Helper()
	runCompose(t, composeFile, "down", "-v")
}

// resetOutput removes and recreates ./output.
func resetOutput(t *testing.T) {
	t.Helper()
	_ = os.RemoveAll("output")
	if err := os.MkdirAll("output", 0755); err != nil {
		t.Fatalf("mkdir output: %v", err)
	}
}

// readIndex returns the map word -> list of doc IDs from ./output/index.
func readIndex(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("output", "index"))
	if err != nil {
		t.Fatalf("cannot read output/index: %v", err)
	}
	result := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		word, docs, ok := strings.Cut(line, "->")
		if !ok {
			continue
		}
		result[strings.TrimSpace(word)] = strings.TrimSpace(docs)
	}
	return result
}

// sortedDocs normalizes a comma-separated doc list: "2,0,1" -> "0,1,2"
func sortedDocs(s string) string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[i] > parts[j] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return strings.Join(parts, ",")
}

// assertIndex checks that word maps to expected sorted doc list.
func assertIndex(t *testing.T, index map[string]string, word, expected string) {
	t.Helper()
	got, ok := index[word]
	if !ok {
		t.Errorf("word %q not found in index", word)
		return
	}
	if sortedDocs(got) != sortedDocs(expected) {
		t.Errorf("word %q: got %q, want %q", word, got, expected)
	}
}

// verifyExpectedIndex checks all expected words are present with correct docs.
func verifyExpectedIndex(t *testing.T, index map[string]string) {
	t.Helper()
	assertIndex(t, index, "uniquealpha", "0")
	assertIndex(t, index, "uniquebeta", "1")
	assertIndex(t, index, "uniquegamma", "2")
	assertIndex(t, index, "uniquedelta", "3")
	assertIndex(t, index, "uniqueepsilon", "4")
	assertIndex(t, index, "testmarker", "0,1,2,3,4")
	assertIndex(t, index, "sharedab", "0,1")
	assertIndex(t, index, "sharedcd", "2,3")
	assertIndex(t, index, "banana", "0,1,4")
	assertIndex(t, index, "cherry", "0,1,2,4")
	assertIndex(t, index, "fox", "0,2,3,4")
}

func TestIntegration_HappyPath(t *testing.T) {
	const composeFile = "docker-compose.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	resetOutput(t)
	runCompose(t, composeFile, "up", "--build", "--abort-on-container-exit")
	time.Sleep(500 * time.Millisecond)

	verifyExpectedIndex(t, readIndex(t))
}

// killContainer runs `docker kill <name>` and t.Fatal on failure.
func killContainer(t *testing.T, name string) {
	t.Helper()
	cmd := exec.Command("docker", "kill", name)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker kill %s failed: %v", name, err)
	}
	t.Logf("killed %s", name)
}

// waitForMaster blocks until mr-master exits, or fails the test on timeout.
func waitForMaster(t *testing.T, timeout time.Duration) {
	t.Helper()
	cmd := exec.Command("docker", "wait", "mr-master")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("docker wait mr-master failed: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("master did not finish within %s", timeout)
	}
}

func TestIntegration_KillDuringMap(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	resetOutput(t)
	runCompose(t, composeFile, "up", "--build", "-d")

	// wait for workers to register + start their doMap (~3s < 8s slow map)
	time.Sleep(3 * time.Second)

	killContainer(t, "mr-worker-2")

	waitForMaster(t, 90*time.Second)
	time.Sleep(500 * time.Millisecond)

	verifyExpectedIndex(t, readIndex(t))
}

func TestIntegration_KillDuringReduce(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	resetOutput(t)
	runCompose(t, composeFile, "up", "--build", "-d")

	// wait until map is done and reduce is in progress
	// map takes ~8s (SIMULATE_SLOW_MAP), reduce starts after map is done
	// SIMULATE_SLOW_REDUCE=5s, so kill ~2s into reduce
	time.Sleep(11 * time.Second)

	killContainer(t, "mr-worker-2")

	waitForMaster(t, 90*time.Second)
	time.Sleep(500 * time.Millisecond)

	verifyExpectedIndex(t, readIndex(t))
}

func TestIntegration_KillTwoWorkers(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	resetOutput(t)
	runCompose(t, composeFile, "up", "--build", "-d")

	// wait until workers are in doMap
	time.Sleep(3 * time.Second)

	killContainer(t, "mr-worker-2")
	killContainer(t, "mr-worker-3")

	// one worker remaining — needs longer timeout
	waitForMaster(t, 120*time.Second)
	time.Sleep(500 * time.Millisecond)

	verifyExpectedIndex(t, readIndex(t))
}
