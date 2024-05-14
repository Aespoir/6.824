package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// cd 6.5840/src/main

// go run mrcoordinator.go pg-*.txt

// go build -buildmode=plugin ../mrapps/wc.go
// rm mr-*
// go run mrworker.go wc.so

// go build -buildmode=plugin ../mrapps/crash.go
// rm mr-*
// go run mrworker.go crash.so

type Coordinator struct {
	// Your definitions here.
	mapTasks    Tasks
	reduceTasks Tasks

	nMap    int
	nReduce int

	mapCh    []chan struct{}
	reduceCh []chan struct{}
}

// 对于map任务：需要 任务ID，files 待处理的文件地址
// 对于reduce任务：需要 任务ID，m个map worker的地址

type Task struct {
	files  []string
	ID     int
	status int8 // 2 means to do, 1 means doing, 0 means done
}

type Tasks struct {
	T        []Task
	mu       sync.Mutex
	finished int
}

func (t *Tasks) TestAndGet() (Task, int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished == len(t.T) {
		return Task{}, 0, true
	}
	for i := range t.T {
		if t.T[i].status == 2 {
			t.T[i].status = 1
			return t.T[i], 1, false
		}
	}
	return Task{}, 0, false
}

func (t *Tasks) IsAllDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.finished == len(t.T)
}

func (t *Tasks) Finish(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.T[id].status == 1 {
		t.T[id].status = 0
		t.finished++
	}
}

// Restart set a task's status to 2, means some worker failed to finish task
func (t *Tasks) Restart(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.T[id].status == 1 {
		t.T[id].status = 2
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if task, n, allDone := c.mapTasks.TestAndGet(); !allDone {
		// buffered chan, for quit
		if n == 0 {
			// all work is doing
			reply.Instruction = "Wait"
			return nil
		}

		reply.Instruction = "Map"
		reply.Files = task.files
		reply.N = c.nReduce
		reply.ID = task.ID

		go c.startMapTask(task.ID, &c.mapTasks)
	} else if task, n, allDone = c.reduceTasks.TestAndGet(); !allDone {
		if n == 0 {
			// all work is doing
			reply.Instruction = "Wait"
			return nil
		}
		go c.startReduceTask(task.ID, &c.reduceTasks)

		reply.Instruction = "Reduce"
		reply.Files = task.files
		reply.N = c.nMap
		reply.ID = task.ID
	} else {
		reply.Instruction = "Exit"
	}
	return nil
}

func (c *Coordinator) startMapTask(id int, tasks *Tasks) {
	quit := time.After(10 * time.Second)

	select {
	case <-c.mapCh[id]:
		tasks.Finish(id)
	case <-quit:
		tasks.Restart(id)
	}
}

func (c *Coordinator) startReduceTask(id int, tasks *Tasks) {
	quit := time.After(10 * time.Second)

	select {
	case <-c.reduceCh[id]:
		tasks.Finish(id)
	case <-quit:
		tasks.Restart(id)
	}
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	// finishMapID := args.ID
	switch args.TaskName {
	case "map":
		c.mapCh[args.ID] <- struct{}{}
	case "reduce":
		c.reduceCh[args.ID] <- struct{}{}
	}

	// log.Printf("%d finish %s work\n", args.ID, args.taskName)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	// log.Printf("start server at %s\n", sockname)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.reduceTasks.IsAllDone() {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
	}

	c.mapCh = make([]chan struct{}, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapCh[i] = make(chan struct{})
	}
	c.reduceCh = make([]chan struct{}, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceCh[i] = make(chan struct{})
	}

	// Your code here.

	// create tasks
	// 均分
	c.mapTasks = Tasks{}
	for i, file := range files {
		c.mapTasks.T = append(c.mapTasks.T,
			Task{files: []string{file}, ID: i, status: 2})
	}

	c.reduceTasks = Tasks{}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks.T = append(c.reduceTasks.T,
			Task{files: []string{}, ID: i, status: 2})
	}

	c.server()
	return &c
}
