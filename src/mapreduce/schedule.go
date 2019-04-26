package mapreduce

import "fmt"
import "sync"

var worker_list = make([]string, 0)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
    fmt.Println("!!! Schedule is being called !!!")
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

    var avaliable_worker = make(chan string)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Your code here (Part 2, 2B).
	//If it's a map task (how can we be sure?)
    go pull_workers(registerChan, avaliable_worker)
    var wg sync.WaitGroup

    taskNumber := 0
    for taskNumber < ntasks {
        worker := <- avaliable_worker
        if(len(worker) > 0) {
            task_args := DoTaskArgs{jobName, mapFiles[taskNumber], mapPhase,
                                    taskNumber, n_other}
            wg.Add(1)
            go start_worker(worker, "Worker.DoTask", task_args, nil,
                            avaliable_worker, &wg)
            taskNumber = taskNumber + 1
        }
    }
    fmt.Printf("Start waiting taskNum: %d ntasks: %d\n", taskNumber, ntasks)
    wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func pull_workers(c chan string, avaliable_worker chan string) {
  for {
    worker := <- c
    worker_list = append(worker_list, worker)
    fmt.Printf("Putting worker %v in avaliable\n", worker)
    avaliable_worker <- worker
  }
}

func start_worker(worker string, rpcname string, task_args interface{},
                  reply interface{}, avaliable_worker chan string, wg *sync.WaitGroup) {
    defer wg.Done()
    fmt.Printf("Worker %v is assigned\n", worker)
    fmt.Println(task_args)
    call(worker, "Worker.DoTask", task_args, reply)
    fmt.Printf("Putting worker %v in avaliable\n", worker)
    avaliable_worker <- worker
}
