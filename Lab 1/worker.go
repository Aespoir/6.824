package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// for loop to ask coordinator for a task
	for {
		reply, err := getTask()
		if err != nil {
			break
		}
		switch reply.Instruction {
		case "Map":
			err = mapTask(mapf, reply.Files, reply.N, reply.ID)
			if err != nil {
				return
			}
			err = finishMap(reply.ID)
			if err != nil {
				return
			}
		case "Reduce":
			err = reduceTask(reducef, reply.N, reply.ID)
			if err != nil {
				return
			}
			err = finishReduce(reply.ID)
			if err != nil {
				return
			}
		case "Merge":
			err = mergeTask(reply.N)
			if err != nil {
				return
			}
			err = finishMerge()
			if err != nil {
				return
			}
			break
		case "Exit":
			break
		default:
			// unknown instruction, break
			break
		}
		time.Sleep(time.Second)
	}

}

func getTask() (GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return GetTaskReply{}, errors.New("call failed!\n")
	}
	return reply, nil
}

func finishMap(id int) error {
	args := FinishArgs{}
	args.ID = id
	reply := FinishReply{}

	ok := call("Coordinator.FinishMap", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return errors.New("call failed!\n")
	}

	return nil
}

func finishReduce(id int) error {
	args := FinishArgs{}
	args.ID = id
	reply := FinishReply{}

	ok := call("Coordinator.FinishReduce", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return errors.New("call failed!\n")
	}

	return nil
}

func finishMerge() error {
	args := FinishArgs{}
	reply := FinishReply{}

	ok := call("Coordinator.FinishMerge", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return errors.New("call failed!\n")
	}

	return nil
}

func mapTask(mapf func(string, string) []KeyValue, files []string, nReduce int, ID int) error {
	nIntermediate := make([][]KeyValue, nReduce)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v\n", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// 执行map函数
		kva := mapf(filename, string(content))
		for _, v := range kva {
			key := ihash(v.Key) % nReduce
			nIntermediate[key] = append(nIntermediate[key], v)
		}
	}
	for i := 0; i < nReduce; i++ {
		// 写入临时文件
		// mr-X-Y, X is the map task number, while Y is the reduce task number
		ofile, err := os.Create(fmt.Sprintf("mr-%d-%d", ID, i))
		if err != nil {
			return err
		}
		for _, v := range nIntermediate[i] {
			_, _ = fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
		}
		_ = ofile.Close()
	}
	fmt.Println("finish map")
	return nil
}

func reduceTask(reducef func(string, []string) string, nMap int, ID int) error {
	intermediate := make([]KeyValue, 0)
	// 从所有mr-nMap-ID文件中读取数据
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, ID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.Split(scanner.Text(), " ")
			intermediate = append(intermediate, KeyValue{line[0], line[1]})
		}
		_ = file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", ID+1))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = intermediate[k].Value
		}
		output := reducef(intermediate[i].Key, values)

		_, _ = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	fmt.Println("finish reduce")
	return nil
}

func mergeTask(nReduce int) error {
	// mr-out-0
	intermediate := make([]KeyValue, 0)
	for i := 1; i <= nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%d", i)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.Split(scanner.Text(), " ")
			intermediate = append(intermediate, KeyValue{line[0], line[1]})
		}
		_ = file.Close()
	}
	sort.Sort(ByKey(intermediate))
	filename := "mr-out-0"
	ofile, _ := os.CreateTemp("", filename)

	for i := 0; i < len(intermediate); i++ {
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	}

	_ = os.Rename(ofile.Name(), filename)
	_ = ofile.Close()

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}