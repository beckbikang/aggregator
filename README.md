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


```

## Uses

```
inner project  focus with parser data and save data into clickhouse

```


