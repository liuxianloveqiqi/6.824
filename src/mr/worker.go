package mr

import (
	"fmt"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 定义 ByKey 结构体，用于排序
type ByKey []KeyValue

// 实现 sort.Interface 接口的 Len 方法
func (bk ByKey) Len() int {
	return len(bk)
}

// 实现 sort.Interface 接口的 Less 方法，按照键进行排序
func (bk ByKey) Less(i, j int) bool {
	return bk[i].Key < bk[j].Key
}

// 实现 sort.Interface 接口的 Swap 方法
func (bk ByKey) Swap(i, j int) {
	bk[i], bk[j] = bk[j], bk[i]
}

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 使用Pid作为Id
	workId := os.Getpid()
	log.Printf("Work id=%v 开始工作", workId)
	// 构建req
	taskId := -1
	taskType := ""
	// 开启循环不断读取任务
	for {
		req := ApplyTaskReq{
			WorkerId: workId,
			TaskId:   taskId,
			TaskType: taskType,
		}
		// 构建resp
		resp := ApplyTaskResp{}
		// TODO 调用Rpc
		call("Coordinator.ApplyForTask", &req, &resp)

		// 根据Map,Reduce类型分发任务
		switch resp.TaskType {
		case "":
			log.Println("已经接收到所有任务")
			// 在switch里面跳出for，使用goto
			goto END
		case Map:
			doMapTask(workId, taskId, resp.NReduce, resp.MapInputFile, mapf)
		case Reduce:

		}
	}
END:
	log.Printf("Worker %d 结束工作\n", workId)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

func doMapTask(workId int, taskId int, NReduce int, fileName string, mapf func(string, string) []KeyValue) {
	// 读取文件数据
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("读取Filename= %v 错误", fileName)
	}
	KeyValues := mapf(fileName, string(content))
	fileMap := map[int][]KeyValue{}
	// 将经过相同的Hash平均分给Reducer
	for _, v := range KeyValues {
		ReducerId := ihash(v.Key) % NReduce
		fileMap[ReducerId] = append(fileMap[ReducerId], v)
	}
	// 写入临时文件
	for i := 0; i < NReduce; i++ {
		outFile, _ := os.Create(tmpMapOutFile(workId, taskId, i))
		for _, v := range fileMap[i] {
			_, err := fmt.Fprintf(outFile, "%v\t%v\n", v.Key, v.Value)
			if err != nil {
				log.Fatalf("map写入 %v号Reducer 临时文件错误", i)
			}
		}
		outFile.Close()
	}
}

// 多个 Map 任务的输出文件中读取数据，合并相同键的值，
// 然后将结果按照键进行排序并写入到 Reduce 任务的输出文件中
func doReducdTask(workId int, taskId int, NMap int, reducef func(string, []string) string) {
	lines := make([]string, 0)
	for i := 0; i < NMap; i++ {
		// 从map临时文件中读取单词
		content, err := os.ReadFile(finalMapOutFile(workId, taskId))
		if err != nil {
			log.Fatalf("读取finalMapOutFile错误")
		}
		// 按照"\n"分割
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	kvs := make([]KeyValue, 0)
	for _, v := range lines {
		// 跳过""
		if strings.TrimSpace(v) == "" {
			continue
		}
		kv := strings.Split(v, "\t")
		kvs = append(kvs, KeyValue{
			Key:   kv[0],
			Value: kv[1],
		})
	}
	// 排序
	sort.Sort(ByKey(kvs))
	outFile, _ := os.Create(tmpReduceOutFile(workId, taskId))
	defer outFile.Close()
	for i := 0; i < len(kvs); {
		j := i + 1
		// 将相同的单词添加到一个切片里面
		for j < len(kvs) && kvs[i] == kvs[j] {
			j++
		}

		vs := make([]string, 0)
		for k := i; k < j; k++ {
			vs = append(vs, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, vs)
		fmt.Fprintf(outFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
}
