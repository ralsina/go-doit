package main

import (
	"fmt"
	"log"

	"github.com/satori/go.uuid"
	"github.com/stevenle/topsort"
)

//Task is a struct describing a task
type Task struct {
	id       string
	name     string
	actions  []string
	fileDep  []string
	targets  []string
	taskDep  []string
	upToDate bool
}

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

	for i, _ := range tasks {
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
		fmt.Println("--->", task.name, task.taskDep)
		for _, name := range task.taskDep {
			fmt.Println("==>", task.id, "==>", name)
			err := graph.AddEdge(task.id, taskNameMap[name])
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	results, err := graph.TopSort(root.id)

	for _, id := range results {
		fmt.Printf("%s -> ", taskIDMap[id].name)
	}

	if err != nil {
		log.Fatal(err)
	}
	return taskIDMap, results
}

func main() {
	t1 := Task{name: "t1"}
	t2 := Task{name: "t2"}
	t3 := Task{name: "t3"}
	t3.taskDep = append(t3.taskDep, "t2")
	t1.taskDep = append(t1.taskDep, "t2")
	t1.taskDep = append(t1.taskDep, "t3")
	tasks := [...]Task{t1, t2, t3}
	ScheduleTasks(tasks[:])
}
