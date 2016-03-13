package mapreduce

import (
	"fmt"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	allDone := make(chan int, ntasks)
	allTask := make(chan int, ntasks)

	for task := 0; task < ntasks; task++ {
		allTask <- task
	}

	// Fork a new goroutine
	go func() {
		for true {
			i := <-allTask
			if i < 0 {
				break
			}

			task := i

			go func() {
				w := <-mr.registerChannel

				taskargs := DoTaskArgs{mr.jobName, mr.files[task], phase, task, nios}
				ok := call(w, "Worker.DoTask", &taskargs, new(struct{}))
				if !ok {
					fmt.Printf("Master: RPC %s[%d] dotask error: %q\n", w, task, ok)
					allDone <- task
				} else {
					allDone <- -1
					mr.registerChannel <- w
				}

			}()
		}
	}()

	for task := 0; task < ntasks; task++ {
		i := <-allDone
		if i >= 0 {
			log.Println("Retry: ", i)
			allTask <- i
			task--
		} else {
			log.Println("Done: ", i)
		}
	}

	allTask <- -1

	fmt.Printf("Schedule: %v phase done\n", phase)
}
