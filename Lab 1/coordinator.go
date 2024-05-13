package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	nMap      int
	wgMap     sync.WaitGroup
	wgReduce  sync.WaitGroup
	wgMerge   sync.WaitGroup
	mWorks    int
	nWorks    int
	finalWork int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// 判断map任务是否全部完成，没完成继续返回map指令
	// 全部完成返回reduce指令
	// 判断reduce是否全部完成，没完成返回reduce指令
	// 全部完成返回please exit指令

	// 简单的先全部分发给一个worker
	if c.mWorks > 0 {
		c.mWorks--
		reply.Instruction = "Map"
		reply.Files = c.files
		reply.N = c.nReduce
		reply.ID = c.mWorks
		c.wgMap.Add(1)
	} else if c.nWorks > 0 {
		c.nWorks--
		c.wgMap.Wait() // 等待全部map任务完成
		reply.Instruction = "Reduce"
		reply.ID = c.nWorks
		reply.N = c.nMap
		c.wgReduce.Add(1)
	} else if c.finalWork > 0 {
		c.wgReduce.Wait()
		c.finalWork--
		c.wgMerge.Add(1)
		reply.Instruction = "Merge"
		reply.N = c.nReduce
	} else {
		reply.Instruction = "Exit"
	}

	return nil
}

// FinishMap
func (c *Coordinator) FinishMap(args *FinishArgs, reply *FinishReply) error {
	// finishMapID := args.ID
	c.wgMap.Done()
	return nil
}

// FinishReduce
func (c *Coordinator) FinishReduce(args *FinishArgs, reply *FinishReply) error {
	// finishReduceID = args.ID
	c.wgReduce.Done()
	return nil
}

// FinishMerge
func (c *Coordinator) FinishMerge(args *FinishArgs, reply *FinishReply) error {
	c.wgMerge.Done()
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.mWorks == 0 && c.nWorks == 0 && c.finalWork == 0 {
		c.wgMerge.Wait()
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:   nReduce,
		nMap:      len(files),
		files:     files,
		wgMap:     sync.WaitGroup{},
		wgReduce:  sync.WaitGroup{},
		wgMerge:   sync.WaitGroup{},
		mWorks:    len(files),
		nWorks:    nReduce,
		finalWork: 1,
	}

	// Your code here.
	c.server()
	return &c
}
