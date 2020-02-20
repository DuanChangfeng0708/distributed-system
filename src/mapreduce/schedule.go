package mapreduce

import (
	"fmt"
	"sync"
)

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
	//
	// Your code here (Part III, Part IV).
	//
	var threadMaster sync.WaitGroup
	for i:=0;i<ntasks;i++{
		//并发分配任务

		threadMaster.Add(1)
		go func(inputFile string,TaskNumber int) {
			//call函数可能失败  循环发送直到发送成功

			for  {
				rpcAdr:=<-registerChan
				ok:=call(rpcAdr,"Worker.DoTask",DoTaskArgs{
					JobName:       jobName,
					File:          inputFile,
					Phase:         phase,
					TaskNumber:   TaskNumber,
					NumOtherPhase: n_other,
				},nil)
				if ok {
					//任务完成后 将此worker重新加入registerChan 表示此woker可用
					//特别注意  一定要先done了只有再往通道里面发送数据，因为此通道是无缓冲通道，当最后一个woker被填入通道
					//而通道对面没有接受，此时此线程会永久阻塞在这里从而无法运行到done，主程序的wait也会永久阻塞，造成死锁
					go func() {
						registerChan<-rpcAdr
					}()
					break
				}
			}
			threadMaster.Done()


		}(mapFiles[i],i)
	}
	fmt.Printf("Schedule: %v done\n", phase)
	threadMaster.Wait()

}
