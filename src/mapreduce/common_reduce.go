package mapreduce

import (
	//"hash/fnv"
    "io/ioutil"
    "os"
    "fmt"
    //"log"
    "encoding/json"
)

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

    //create generic json
    var intermediate_struct map[string][]string

    //fmt.Println("nMap ", nMap)
    for mapNumber := 0; mapNumber < nMap; mapNumber ++ {
        inFile := reduceName(jobName, mapNumber, reduceTask)
        //fmt.Println("mapNumber: ", mapNumber, " inFile: ", inFile)
        dat, err := ioutil.ReadFile(inFile)
        e_check(err)
        var tmpJson []KeyValue
        //fmt.Println(dat)
        json.Unmarshal([]byte(dat), &tmpJson)
        fmt.Println(tmpJson)

        //Write to giant json file
        for _, kv:= range tmpJson {
            //Add each key value pair
            intermediate_struct[kv.Key] = append(intermediate_struct[kv.Key],kv.Value)
            /*
            if !(intermediate_struct[kv.Key]) {
                intermediate_struct[kv.Key] := [kv.Value]
            }
            else {
                intermediate_struct[kv.Key] = append(intermediate_struct[kv.Key],kv.Value)
            }*/
        }
    }

    var outputs []string
    //After finishing building intermediate_struct, call reduceF on every key
    for k, v := range intermediate_struct {
        reduce_output := reduceF(k, v)
        outputs = append(outputs, reduce_output)
        //fmt.Println("k:", k, "v:", v)
    }


    //Write to file
    var jsonData []byte
    jsonData, err := json.Marshal(&outputs)
    e_check(err)
    f, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    e_check(err)
    _, err = f.WriteString(string(jsonData))
    e_check(err)
    f.Close()





    //Now that we have json file with a ton of redundant keys and values
    //Get the same keys and reduce them

}
