package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

)


type keyValues []KeyValue
func (s keyValues) Len() int {return len(s)}
func (s keyValues) Swap(i,j int){s[i],s[j]=s[j],s[i]}
func (s keyValues) Less(i,j int) bool{return s[i].Key<s[j].Key}
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//读取输入文件内容
	var reduceDate []KeyValue

	//打开-所有-中间文件
	var ReduceInputFileName string
	for i:=0;i<nMap ;i++  {
		//获取输入文件名
		ReduceInputFileName=reduceName(jobName,i,reduceTask)
		file,err:=os.Open(ReduceInputFileName)
		if err!=nil {
			fmt.Println(ReduceInputFileName,"reduce 71 文件打开失败:",err)
		}

		//json 解码 必须循环获取  不能按照网上的传入结构体切片
		decoder :=json.NewDecoder(file)
		for {
			var v KeyValue
			err2:=decoder.Decode(&v)
			if err2 != nil {
				break
			}
			reduceDate = append(reduceDate, v)
		}
		//最后关闭文件 不要用defer
		file.Close()
	}


	//对输入内容排序 使用函数库里面的sort进行排序
	var SortedReduceDate keyValues=reduceDate[:]
	sort.Sort(SortedReduceDate)

	//准备输出文件
	ReduceOutFile,err1:=os.OpenFile(outFile,os.O_WRONLY|os.O_APPEND|os.O_CREATE,0644)
	if err1!=nil {
		fmt.Println(outFile,"reduce 70 文件打开失败:",err1)
	}
	defer ReduceOutFile.Close()

	//json 对输出内容文件编码输出
	encoder:=json.NewEncoder(ReduceOutFile)
	var dateForPerKey []string
	key:=SortedReduceDate[0].Key
	for _,value:=range SortedReduceDate {
		if key==value.Key{
			dateForPerKey=append(dateForPerKey,value.Value)
		} else{
			outString:=reduceF(key,dateForPerKey)
			encoder.Encode(KeyValue{key,outString})
			key=value.Key
			//清空上一个dateForPerKey
			dateForPerKey=dateForPerKey[:0]
			dateForPerKey=append(dateForPerKey,value.Value)
		}

	}
	//最后一个key
	if(len(dateForPerKey)!=0){
		outString:=reduceF(key,dateForPerKey)
		encoder.Encode(KeyValue{key,outString})
	}




}
