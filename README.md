# aggregator


gather data from any way and deal it in some go goroutine

收集数据，然后通过批量处理


it's all right for clickhouse  or mysql and etc ...


we can gather data together and insert batch

特别适合批量收集，集中处理


## Installation

```
https://github.com/beckbikang/aggregator
```

## how to use

```
na := NewAggregator(workCount, chanSize, processCount int )

workCount pool size   池的大小，goroutine个数
chanSize  receive chan size  缓存chan的大小
processCount   batch size   批量处理的数据的大小


na.SetHandler()
na.SetErrorHandler()
na.Start()

na.Receive(msg)

na.Stop()



```

## Example

```
package main

import (
	"time"
	"fmt"
	"github.com/beckbikang/aggregator"
)

func main() {

	type Iv struct{
		j int
	}

	ag := aggregator.NewAggregater(2, 10, 20)
	ag.SetErrorHandler(func(msg string) {
		fmt.Println(msg)
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
		fmt.Printf("%+v\n", list)

		var list2 []int
		for _,v1 := range list {
			list2 = append(list2, v1.j)
		}
		fmt.Printf("%+v\n", list2)

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

}

```

## Uses

```
inner project  focus with parser data and save data into clickhouse

```


