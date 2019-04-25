package mapreduce

import "fmt"

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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Your code here (Part 2, 2B).
	//If it's a map task (how can we be sure?)
	for taskNumber := 0; taskNumber < ntasks; taskNumber ++ {
		//Reading registerChan argument
		worker_rpc_address := <-registerChan

		//RPC's arguments are defined by DoTaskArgs in mapreduce/common_rpc.go
		task_args:=DoTaskArgs{jobName,mapFiles[taskNumber],mapPhase,taskNumber,n_other}

		//Send RPC to worker
		go call(worker_rpc_address, "Worker.DoTask",task_args, nil)

}



	fmt.Printf("Schedule: %v done\n", phase)
}
