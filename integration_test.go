//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testDocs — контент для test-input/doc{i}.txt, i = 0..4.
// Каждый файл содержит уникальный маркер "unique<X>" + общий "testmarker",
// плюс пересекающиеся слова для проверки корректного мержа.
var testDocs = []string{
	"uniquealpha testmarker fox cherry sharedab banana",
	"uniquebeta testmarker sharedab cherry banana",
	"uniquegamma testmarker fox cherry sharedcd",
	"uniquedelta testmarker fox sharedcd",
	"uniqueepsilon testmarker fox cherry banana",
}

const (
	testInputDir = "input"
	bucketName   = "mapreduce"
)

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

func cleanupCluster(t *testing.T, composeFile string) {
	t.Helper()
	full := []string{"compose", "-f", composeFile, "--profile", "manual", "down", "-v"}
	cmd := exec.Command("docker", full...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

// writeTestInputs очищает ./input/ и заполняет его testDocs.
func writeTestInputs(t *testing.T) []string {
	t.Helper()
	_ = os.RemoveAll(testInputDir)
	if err := os.MkdirAll(testInputDir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", testInputDir, err)
	}
	names := make([]string, len(testDocs))
	for i, content := range testDocs {
		name := fmt.Sprintf("doc%d.txt", i)
		path := filepath.Join(testInputDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		names[i] = name
	}
	return names
}

// runClientContainer запускает клиента одноразово и ждёт его завершения.
// Возвращает код выхода (0 = job done).
func runClientContainer(t *testing.T, composeFile string, docNames []string) int {
	t.Helper()
	args := []string{
		"compose", "-f", composeFile,
		"--profile", "manual",
		"run", "--rm", "client",
		"--mode=client",
		"--master_addr=master:50051",
	}
	for _, name := range docNames {
		args = append(args, "/data/"+name)
	}
	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	t.Fatalf("docker compose run client failed to launch: %v", err)
	return -1
}

// fetchFinalIndex скачивает финальный index из MinIO во временный файл
// и возвращает его содержимое как map word -> comma-separated docs.
func fetchFinalIndex(t *testing.T, jobID int) map[string]string {
	t.Helper()

	// mc client в отдельном контейнере, подключённом к той же сети.
	// Пробуем оба возможных имени сети (docker compose использует projectname_default).
	networkName := detectNetwork(t)

	tmpDir := t.TempDir()
	// mc копирует объект в /out/final; монтируем tmpDir в /out.
	// Работаем от uid=1000, чтобы файл не оказался с root-правами.
	uid := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())

	key := fmt.Sprintf("%s/index/%d/final", bucketName, jobID)

	script := fmt.Sprintf(
		"mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && "+
			"mc cp local/%s /out/final",
		key,
	)

	cmd := exec.Command("docker", "run", "--rm",
		"--network", networkName,
		"--user", uid,
		"-v", tmpDir+":/out",
		"--entrypoint", "sh",
		"minio/mc:latest",
		"-c", script,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("mc cp failed: %v\noutput:\n%s", err, out.String())
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, "final"))
	if err != nil {
		t.Fatalf("read final index: %v", err)
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

// detectNetwork ищет docker network, в которой поднят minio.
func detectNetwork(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("docker", "inspect", "mapreduce-minio-1", "--format", "{{json .NetworkSettings.Networks}}")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker inspect minio: %v", err)
	}
	var nets map[string]any
	if err := json.Unmarshal(out, &nets); err != nil {
		t.Fatalf("parse networks: %v", err)
	}
	for name := range nets {
		return name
	}
	t.Fatalf("no networks found for minio container")
	return ""
}

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

// verifyExpectedIndex проверяет что все ожидаемые слова присутствуют
// с корректными списками документов.
// Doc IDs — задаются в клиенте как index в flag.Args, то есть 0..4.
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

	docs := writeTestInputs(t)
	runCompose(t, composeFile, "up", "--build", "-d", "master", "worker-1", "worker-2", "worker-3", "minio")
	// дать воркерам зарегистрироваться
	time.Sleep(3 * time.Second)

	if code := runClientContainer(t, composeFile, docs); code != 0 {
		t.Fatalf("client exited with code %d, expected 0", code)
	}

	verifyExpectedIndex(t, fetchFinalIndex(t, 0))
}

// killContainer посылает docker kill указанному контейнеру.
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

func TestIntegration_KillDuringMap(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	docs := writeTestInputs(t)
	runCompose(t, composeFile, "up", "--build", "-d", "master", "worker-1", "worker-2", "worker-3", "minio")
	time.Sleep(3 * time.Second)

	// клиент в фоне
	clientDone := make(chan int, 1)
	go func() { clientDone <- runClientContainer(t, composeFile, docs) }()

	// подождать пока map стартует (~2s после начала workflow, SLOW_MAP=8s)
	time.Sleep(5 * time.Second)
	killContainer(t, "mr-worker-2")

	select {
	case code := <-clientDone:
		if code != 0 {
			t.Fatalf("client exited with code %d, expected 0", code)
		}
	case <-time.After(120 * time.Second):
		t.Fatalf("client did not finish within 120s")
	}

	verifyExpectedIndex(t, fetchFinalIndex(t, 0))
}

func TestIntegration_KillDuringReduce(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	docs := writeTestInputs(t)
	runCompose(t, composeFile, "up", "--build", "-d", "master", "worker-1", "worker-2", "worker-3", "minio")
	time.Sleep(3 * time.Second)

	clientDone := make(chan int, 1)
	go func() { clientDone <- runClientContainer(t, composeFile, docs) }()

	// подождать пока map закончится и начнётся reduce (~13s)
	time.Sleep(13 * time.Second)
	killContainer(t, "mr-worker-2")

	select {
	case code := <-clientDone:
		if code != 0 {
			t.Fatalf("client exited with code %d, expected 0", code)
		}
	case <-time.After(120 * time.Second):
		t.Fatalf("client did not finish within 120s")
	}

	verifyExpectedIndex(t, fetchFinalIndex(t, 0))
}

func TestIntegration_KillTwoWorkers(t *testing.T) {
	const composeFile = "docker-compose.slow.yml"
	cleanupCluster(t, composeFile)
	t.Cleanup(func() { cleanupCluster(t, composeFile) })

	docs := writeTestInputs(t)
	runCompose(t, composeFile, "up", "--build", "-d", "master", "worker-1", "worker-2", "worker-3", "minio")
	time.Sleep(3 * time.Second)

	clientDone := make(chan int, 1)
	go func() { clientDone <- runClientContainer(t, composeFile, docs) }()

	time.Sleep(5 * time.Second)
	killContainer(t, "mr-worker-2")
	killContainer(t, "mr-worker-3")

	select {
	case code := <-clientDone:
		if code != 0 {
			t.Fatalf("client exited with code %d, expected 0", code)
		}
	case <-time.After(180 * time.Second):
		t.Fatalf("client did not finish within 180s")
	}

	verifyExpectedIndex(t, fetchFinalIndex(t, 0))
}

