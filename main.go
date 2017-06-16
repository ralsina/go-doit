package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"

	"reflect"

	"github.com/asdine/storm"
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
	taskDep  mapset.Set
	upToDate bool
}

//TaskMap is a map of tasks indexed by string
type TaskMap map[string]Task

// ScheduleTasks sorts tasks on order of execution to satisfy
// dependencies.
func ScheduleTasks(tasks []Task, db *storm.DB) []Task {
	taskIDMap := make(map[string]Task)
	taskNameMap := make(map[string]string)
	root := Task{
		id:      uuid.NewV4().String(),
		name:    "root",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
		taskDep: mapset.NewSet(),
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
			log.Fatal("Error adding edge: ", err)
		}
	}

	// Add edges by task dependency
	for _, task := range tasks {
		for name := range task.taskDep.Iter() {
			err := graph.AddEdge(task.id, taskNameMap[name.(string)])
			if err != nil {
				log.Fatal("Error adding edge: ", err)
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
	idResults, err := graph.TopSort(root.id)
	if err != nil {
		log.Fatal("Error sorting tasks: ", err)
	}

	// Re-map IDs to tasks
	taskResults := make([]Task, len(tasks)+1)
	for i := range idResults {
		taskResults[i] = taskIDMap[idResults[i]]
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

// dirty calculates if a task deps have changed since last run
func dirty(task Task, db *storm.DB) bool {
	old := GetLastDepData(task, db)
	new := CalculateDepData(task)
	return !reflect.DeepEqual(old, new)
}

func main() {
	db := InitDB("my.db")
	defer db.Close()

	t1 := Task{
		name:    "t1",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
		taskDep: mapset.NewSet(),
	}
	t2 := Task{
		name:    "t2",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
		taskDep: mapset.NewSet(),
	}
	t3 := Task{
		name:    "t3",
		fileDep: mapset.NewSet(),
		targets: mapset.NewSet(),
		taskDep: mapset.NewSet(),
	}
	t1.fileDep.Add("f1")
	t1.fileDep.Add("f2")
	t3.targets.Add("f2")
	t2.targets.Add("f1")
	tasks := [...]Task{t1, t2, t3}
	r := ScheduleTasks(tasks[:], db)
	for _, t := range r {
		UpdateDepData(t, db)
		fmt.Printf("%v(%v) ->", t.name, dirty(t, db))
	}
}
