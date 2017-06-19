package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"

	"reflect"

	"github.com/asdine/storm"
	"github.com/deckarep/golang-set"
	"github.com/stevenle/topsort"
)

//Task is a struct describing a task
type Task struct {
	name string
	// fileDep is the set of files that this task depends on
	fileDep mapset.Set
	// targets is the set of files this task generates
	targets mapset.Set
	// taskDep is the set of tasks this task requires be ran BEFORE it
	taskDep mapset.Set
}

//TaskMap is a map of tasks indexed by string
type TaskMap map[string]Task

// ScheduleTasks sorts tasks on order of execution to satisfy
// dependencies. It also removes all tasks that have their
// dependencies unchanged since last successful run.
func ScheduleTasks(tasks []Task, db *storm.DB) []Task {
	taskNameMap := make(map[string]Task)
	root := Task{
		name:    "root",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
		taskDep: mapset.NewSet(),
	}
	taskNameMap[root.name] = root
	graph := topsort.NewGraph()
	graph.AddNode(root.name)

	for i := range tasks {
		// Assign unique UUIDs to all tasks
		taskNameMap[tasks[i].name] = tasks[i]
		// Create task nodes
		graph.AddNode(tasks[i].name)
		// Root depends on all tasks
		err := graph.AddEdge(root.name, tasks[i].name)
		if err != nil {
			log.Fatal("Error adding edge: ", err)
		}
	}

	// Add edges by task dependency
	for _, task := range tasks {
		for name := range task.taskDep.Iter() {
			err := graph.AddEdge(task.name, name.(string))
			if err != nil {
				log.Fatal("Error adding edge: ", err)
			}
		}
	}

	// Sanity check: fileDeps either exist or are targets
	allTargets := mapset.NewSet()
	for i := range tasks {
		allTargets = allTargets.Union(tasks[i].targets)
	}
	for i := range tasks {
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
	for i, t1 := range tasks {
		for _, t2 := range tasks[i+1:] {
			t1tc := t1.targets.Cardinality()
			t2tc := t2.targets.Cardinality()
			if (t1tc > 0) && (t2tc > 0) {
				intersection := t1.targets.Intersect(t2.targets)
				if intersection.Cardinality() > 0 {
					log.Fatalf("Tasks %s and %s share targets: %s", t1.name, t2.name, intersection)
				}
			}

			if t2tc > 0 {
				intersection := t1.fileDep.Intersect(t2.targets)
				if intersection.Cardinality() > 0 {
					graph.AddEdge(t1.name, t2.name)
				}
			}

			if t1tc > 0 {
				intersection := t2.fileDep.Intersect(t1.targets)
				if intersection.Cardinality() > 0 {
					graph.AddEdge(t2.name, t1.name)
				}
			}
		}
	}

	// Sort topologically and return
	nameResults, err := graph.TopSort(root.name)
	if err != nil {
		log.Fatal("Error sorting tasks: ", err)
	}

	// Re-map IDs to tasks
	taskResults := make([]Task, len(tasks)+1)
	for i := range nameResults {
		taskResults[i] = taskNameMap[nameResults[i]]
	}
	results := FilterTasks(taskResults, db)
	return results
}

// InitDB creates/opens a Storm DB to store up-to-date data
func InitDB(path string) *storm.DB {
	db, err := storm.Open(path)
	if err != nil {
		log.Fatal("Error opening DB: ", err)
	}
	return db
}

// hashFile calculates the md5 hash of a file
func hashFile(path string) string {
	// FIles that don't exist have invalud hashes
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error opening file %s: %s", path, err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatalf("Error reading file %s: %s", path, err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// FilterTasks takes a list of tasks and return tasks that are not up to date.
func FilterTasks(tasks []Task, db *storm.DB) []Task {
	result := make([]Task, 0)
	for _, t := range tasks {
		if dirty(t, db) {
			result = append(result, t)
		}
	}
	return result
}

// DepData describes both a task and its file dependencies state
type DepData struct {
	ID         string
	fileHashes map[string]string
}

// CalculateDepData creates a DepData struct for a given task matching the
// current state of the universe.
func CalculateDepData(task Task) DepData {
	hashes := make(map[string]string)
	for path := range task.fileDep.Iter() {
		hashes[path.(string)] = hashFile(path.(string))
	}
	return DepData{
		ID:         task.name,
		fileHashes: hashes,
	}
}

// GetLastDepData gets the last state for a task as stored in the database.
func GetLastDepData(task Task, db *storm.DB) DepData {
	result := DepData{
		ID: task.name,
	}
	db.Get("ID", task.name, &result.fileHashes)
	// TODO: handle error
	return result
}

// UpdateDepData stores current state for a task into the DB
func UpdateDepData(task Task, db *storm.DB) {
	data := CalculateDepData(task)
	err := db.Set("ID", task.name, &data.fileHashes)
	if err != nil {
		log.Fatal("Error saving data to DB: ", err)
	}
}

// dirty calculates if a task needs to run again. That can be because:
// * depFiles have changed since last successful run
// * This task has never run before
// * The targets of the task don't exist
// TODO: a dirty task has a target that is a fileDep of this task (and so on)
func dirty(task Task, db *storm.DB) bool {
	old := GetLastDepData(task, db)
	new := CalculateDepData(task)
	depsChanged := !reflect.DeepEqual(old, new)
	if depsChanged {
		return true
	}
	// If any fileDep doesn't exist, task is dirty
	for path := range task.fileDep.Iter() {
		if _, err := os.Stat(path.(string)); os.IsNotExist(err) {
			return true
		}
	}

	// If any target doesn't exist, task is dirty
	for path := range task.targets.Iter() {
		if _, err := os.Stat(path.(string)); os.IsNotExist(err) {
			return true
		}
	}
	return false
}

func main() {

	f, err := os.Create("cosa.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	db := InitDB("my.db")
	db.Bolt.NoSync = true
	defer db.Close()

	count := 1000

	tasks := make([]Task, count)

	for i := 0; i < count; i++ {
		tasks[i] = Task{
			name:    fmt.Sprintf("task-%d", i),
			fileDep: mapset.NewSet(),
			targets: mapset.NewSet(),
			taskDep: mapset.NewSet(),
		}
	}
	fmt.Printf("Scheduling %d tasks\n", count)
	// TODO: cleanup tasks that don't exist anymore
	r := ScheduleTasks(tasks[:], db)
	for _, t := range r {
		UpdateDepData(t, db)
		// fmt.Printf("%v(%v) ->", t.name, dirty(t, db))
	}
	fmt.Printf("Done.\n")
}
