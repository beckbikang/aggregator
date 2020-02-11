package aggregator

import (
	"testing"
	"time"
)

func TestWork(t *testing.T){

	type Iv struct{
		j int
	}

	ag := NewAggregater(2, 10, 20)
	ag.SetErrorHandler(func(msg string) {
		t.Log(msg)
	})
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


	for i:=0; i < 100;i++{
		var iv Iv
		iv.j = i

		go func( iv *Iv){
			ag.Receive(iv)
			time.Sleep(time.Millisecond * 50)
		}(&iv)
	}

	time.Sleep(time.Second * 1)
	ag.Stop()
}
