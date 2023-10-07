package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Lock      sync.Mutex
	TaskType  string
	NMap      int
	NReduce   int
	Tasks     map[string]Task
	ToDoTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and resp types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, resp *ExampleReply) error {
	resp.Y = args.X + 1
	return nil
}

// crateTaskId 根据任务类型和任务编号创建唯一的任务ID
func crateTaskId(taskType string, taskId int) string {
	return fmt.Sprintf("%s-%d", taskType, taskId)
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
	//ret := false
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// Your code here.

	return c.TaskType == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Lock:      sync.Mutex{},
		TaskType:  Map,
		NMap:      len(files),
		NReduce:   nReduce,
		Tasks:     map[string]Task{},
		ToDoTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.

	for k, v := range files {
		task := Task{
			Id:           k,
			Type:         Map,
			WorkerId:     -1,
			MapInputFile: v,
			DeadLine:     time.Time{},
		}
		log.Printf("Type: %s", task.Type)
		c.Tasks[crateTaskId(task.Type, task.Id)] = task
		c.ToDoTasks <- task
	}
	c.server()
	// 开启携程循环，回收超时任务
	// 后台周期性地检查任务的截止时间
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			// 加锁，防止并发访问Map
			c.Lock.Lock()
			for _, task := range c.Tasks {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					log.Printf(" Worker %d 运行任务 %s %d 出现故障，重新收回！", task.WorkerId, task.Type, task.Id)
					// 重新分配任务
					task.WorkerId = -1
					c.ToDoTasks <- task
				}
			}
			c.Lock.Unlock()
		}
	}()
	return &c
}

func (c *Coordinator) ApplyForTask(req *ApplyTaskReq, resp *ApplyTaskResp) error {
	// 先检查工作是否完成
	if req.TaskId != -1 {
		// 确保多个请求不会同时修改任务状态。
		c.Lock.Lock()
		taskId := crateTaskId(req.TaskType, req.TaskId)
		// 检查是否存在具有相同任务ID和工作节点ID的任务，避免不合法的工作节点试图报告不属于它的任务完成情况。
		if task, ok := c.Tasks[taskId]; ok && task.WorkerId == req.WorkerId {
			log.Printf("%d 完成 %s-%d 任务", req.WorkerId, req.TaskType, req.TaskId)
			// 如果上一个任务类型是 MAP，则遍历每个 Reduce 任务的输出文件，将其重命名为最终的 Map 输出文件。
			if req.TaskType == Map {
				for i := 0; i < c.NReduce; i++ {
					err := os.Rename(tmpMapOutFile(req.WorkerId, req.TaskId, i), finalMapOutFile(req.TaskId, i))
					if err != nil {
						fmt.Println(req)
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(req.WorkerId, req.TaskId, i), err)
					}
				}

			} else if req.TaskType == Reduce {
				err := os.Rename(
					tmpReduceOutFile(req.WorkerId, req.TaskId),
					finalReduceOutFile(req.TaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(req.WorkerId, req.TaskId), err)
				}
			}
			// 删除任务映射表
			delete(c.Tasks, taskId)
			if len(c.Tasks) == 0 {
				c.cutover()
			}
			c.Lock.Unlock()
		}
	}
	task, ok := <-c.ToDoTasks
	if !ok {
		return nil
	}
	c.Lock.Lock()
	defer c.Lock.Unlock()
	log.Printf("Assign %s task %d to worker %dls"+
		"\n", task.Type, task.Id, req.WorkerId)
	task.WorkerId = req.WorkerId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.Tasks[crateTaskId(task.Type, task.Id)] = task
	// 给work返回数据
	resp.TaskId = task.Id
	resp.TaskType = task.Type
	resp.MapInputFile = task.MapInputFile
	resp.NMap = c.NMap
	resp.NReduce = c.NReduce
	return nil
}

// Map,Reduce 切换
func (c *Coordinator) cutover() {
	if c.TaskType == Map {
		log.Printf("所有的MAP任务已经完成！开始REDUCE任务！")
		c.TaskType = Reduce
		for i := 0; i < c.NReduce; i++ {
			task := Task{
				Id:           i,
				Type:         Reduce,
				WorkerId:     -1,
				MapInputFile: "",
				DeadLine:     time.Time{},
			}
			c.Tasks[crateTaskId(task.Type, task.Id)] = task
			c.ToDoTasks <- task
		}
	} else if c.TaskType == Reduce {
		log.Printf("所有的REDUCE任务已经完成！")
		close(c.ToDoTasks)
		c.TaskType = Done
	}
}
