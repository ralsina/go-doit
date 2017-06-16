package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"github.com/deckarep/golang-set"
	"github.com/satori/go.uuid"
	"github.com/stevenle/topsort"
)

//Task is a struct describing a task
type Task struct {
	id       string
	name     string
	fileDep  mapset.Set
	targets  mapset.Set
	taskDep  []string
	upToDate bool
}

//TaskMap is a map of tasks indexed by string
type TaskMap map[string]Task

// ScheduleTasks sorts tasks on order of execution to satisfy
// dependencies.
func ScheduleTasks(tasks []Task) (TaskMap, []string) {
	taskIDMap := make(map[string]Task)
	taskNameMap := make(map[string]string)
	root := Task{
		id:   uuid.NewV4().String(),
		name: "root",
	}
	taskIDMap[root.id] = root
	graph := topsort.NewGraph()
	graph.AddNode(root.id)

	for i := range tasks {
		// Assign unique UUIDs to all tasks
		tasks[i].id = uuid.NewV4().String()
		taskIDMap[tasks[i].id] = tasks[i]
		taskNameMap[tasks[i].name] = tasks[i].id
		// Create task nodes
		graph.AddNode(tasks[i].id)
		// Root depends on all tasks
		err := graph.AddEdge(root.id, tasks[i].id)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Add edges by task dependency
	for _, task := range tasks {
		for _, name := range task.taskDep {
			err := graph.AddEdge(task.id, taskNameMap[name])
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// Sanity check: targets can't be repeated
	for i, t1 := range tasks {
		for _, t2 := range tasks[i+1:] {
			inter := t1.targets.Intersect(t2.targets)
			if inter.Cardinality() > 0 {
				log.Fatalf("Tasks %s and %s share targets: %s", t1.name, t2.name, inter)
			}
		}
	}

	// Sanity check: fileDeps either exist or are targets
	allTargets := mapset.NewSet()
	for i := range(tasks) {
		allTargets = allTargets.Union(tasks[i].targets)
	}
	for i := range(tasks) {
		// What deps are NOT targets?
		notTargets := tasks[i].fileDep.Difference(allTargets)
		// All these must exist
		for path := range notTargets.Iter() {
			if _, err := os.Stat(path.(string)); os.IsNotExist(err) {
				log.Fatalf("Path %s is a dependency of task %s and is missing.", path, tasks[i].name)
			}
		}
	}

	// Add edges by fileDep/target relationship
	for _, source := range tasks {
		for _, dest := range tasks {
			for targetFile := range dest.targets.Iter() {
				if source.fileDep.Contains(targetFile) {
					graph.AddEdge(source.id, dest.id)
				}
			}
		}
	}

	// Sort topologically and return
	results, err := graph.TopSort(root.id)

	if err != nil {
		log.Fatal(err)
	}
	return taskIDMap, results
}

// InitDB creates/opens a BoltDB to store up-to-date data
func InitDB(path string) *bolt.DB {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func hashFile(path string) string {
	f, err := os.Open("file.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return string(h.Sum(nil))
}

// dirty calculates if a task deps have changed since last run
func dirty(task Task) bool {
	// TODO: Implement
	return true
}

// FilterTasks takes a list of tasks and return tasks that are not up to date.
func FilterTasks(tasks []Task, db *bolt.DB) []Task{
	result := make([]Task, len(tasks))
	for i := range tasks {
		if dirty(tasks[i]) {
			result = append(result, tasks[i])
		}
	}
	return result
}

type DepData struct {
	fileHashes map[string]string
}

func CalculateDepData(task Task) DepData {
	hashes := make(map[string]string)
	for path := range task.fileDep.Iter() {
		hashes[path.(string)] = hashFile(path.(string))
	}
	return DepData {
		fileHashes: hashes,
	}
}

func main() {
	db := InitDB("my.db")
	defer db.Close()

	t1 := Task{
		name:    "t1",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
	}
	t2 := Task{
		name:    "t2",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
	}
	t3 := Task{
		name:    "t3",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
	}
	t1.fileDep.Add("f1")
	t1.fileDep.Add("f2")
	t3.targets.Add("f3")
	// t2.targets.Add("f1")
	t2.targets.Add("f2")
	tasks := [...]Task{t1, t2, t3}
	m, r := ScheduleTasks(tasks[:])
	for _, id := range r {
		fmt.Printf("%s -> ", m[id].name)
	}
}
