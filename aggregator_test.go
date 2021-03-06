package aggregator

import (
	"testing"
	"time"
	"sync/atomic"
)

//test stop
func TestStopAggregator(t *testing.T){

	ag := NewAggregator(2, 10, 20)
	ag.SetErrorHandler(func(msg string) {
		t.Log(msg)
	})
	ag.StopHandle()

	if !ag.IsStopped(){
		t.Error("stop failed")
	}
}

//test work
func TestWork(t *testing.T){

	type Iv struct{
		j int
	}

	var addCount uint32 = 0

	ag := NewAggregator(2, 10, 20)
	ag.SetErrorHandler(func(msg string) {
		t.Log(msg)
	})
	ag.SetTickFrequency(time.Millisecond * 100)
	ag.SetHandler(func(v []interface{})error{

		var list []*Iv

		for _,v1 := range v{
			var v2 *Iv
			var ok bool
			v2,ok = v1.(*Iv)
			if !ok {
				continue
			}
			list = append(list, v2)
			atomic.AddUint32(&addCount,1)
		}
		t.Logf("%+v", list)

		var list2 []int
		for _,v1 := range list {
			list2 = append(list2, v1.j)
		}
		t.Logf("%+v", list2)


		return nil
	})

	ag.Start()

	addTimes := 100
	for i := 0; i < addTimes;i++{
		var iv Iv
		iv.j = i

		go func( iv *Iv){
			ag.Receive(iv)
			time.Sleep(time.Millisecond * 5)
		}(&iv)
	}

	time.Sleep(time.Second * 1)
	ag.Stop()

	ag.Receive("test")

	if uint32(addTimes) != addCount {
		t.Fatalf("addTimes:%d != addCount:%d\n", addTimes, addCount)
	}

	//do left
	for i:=0; i< addTimes;i++ {
		var iv Iv
		iv.j = i
		ag.aggregatorData = append(ag.aggregatorData, &iv)
	}
	ag.handleLeft()


}


