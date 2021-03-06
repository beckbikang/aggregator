package aggregator

import (
	"time"
	"fmt"
	"sync"
	"sync/atomic"
)


const DefaultTickTime = time.Millisecond * 500

//aggregation to deal
type aggregator struct{
	wait *sync.WaitGroup
	handler func([] interface{})error
	errorHandler func(msg string)
	aggregatorData []interface{}
	processCount int //process count
	lock *sync.Mutex
	workCount int
	dataChan chan interface{}
	stopped chan struct{}
	isStop int32
	tickFrequency time.Duration
}

//create object
func NewAggregator(workCount, chanSize, processCount int )*aggregator{
	return &aggregator{
		wait:&sync.WaitGroup{},
		workCount:workCount,
		processCount:processCount,
		dataChan: make(chan interface{}, chanSize),
		stopped: make(chan struct{}),
		isStop: 0,
		aggregatorData:make([]interface{},0),
		lock: &sync.Mutex{},
		tickFrequency:DefaultTickTime,
	}
}

//set handler
func (a *aggregator)SetHandler(handler func([] interface{})error){
	a.handler = handler
}

//set error handler
func (a *aggregator)SetErrorHandler(errorHandler func(msg string)){
	a.errorHandler = errorHandler
}

func (a *aggregator)SetTickFrequency(duration time.Duration){
	a.tickFrequency = duration
}

//receive data
func (a *aggregator)Receive(data interface{}){
	//not close
	if a.IsStopped(){
		return
	}
	a.dataChan<-data
}

//start pool
func (a *aggregator) Start(){

	for i:=0; i < a.workCount; i++ {
		go func(){
			a.wait.Add(1)
			worker(a)
		}()
	}
}

//do some worker
func worker(a *aggregator){
	defer a.wait.Done()



	defer func(){

		if err := recover(); err != nil {
			if a.errorHandler != nil {
				a.errorHandler(fmt.Sprintf("aggregaterData has not deal %d", len(a.aggregatorData)))
			}
		}
	}()

	ticker := time.NewTicker(a.tickFrequency)
	var toBeProcess []interface{}

loop:
	for{
		select {
		case <-a.stopped:
			a.addData(toBeProcess)
			a.StopHandle()
			break loop
		case data,ok := <- a.dataChan:
			if a.IsStopped() || !ok{
				a.addData(toBeProcess)
				a.StopHandle()
				break loop
			}else{
				toBeProcess = append(toBeProcess, data)
				if len(toBeProcess) >= a.processCount {
					err := a.handler(toBeProcess)
					if err == nil {
						toBeProcess = make([]interface{}, 0)
					}else {
						if a.errorHandler != nil {

						}
						a.errorHandler(fmt.Sprintf("aggregate handler msg failed %+v",toBeProcess))
					}
				}
			}
		case <-ticker.C:
			if a.IsStopped(){
				a.addData(toBeProcess)
				break loop
			}
		}
	}
}

//add data
func (a *aggregator) addData(toBeProcess []interface{}){
	if len(toBeProcess) > 0{
		a.lock.Lock()
		a.aggregatorData = append(a.aggregatorData, toBeProcess...)
		a.lock.Unlock()
	}
}

//stop
func (a *aggregator)StopHandle(){
	atomic.StoreInt32(&(a.isStop),1)
}

//check is stopped
func (a *aggregator)IsStopped()bool{
	return atomic.LoadInt32(&(a.isStop)) == 1
}

//stop
func (a *aggregator) Stop(){
	a.stopped <- struct{}{}
	close(a.dataChan)
	a.wait.Wait()

	//range data chan
	for v := range a.dataChan{
		a.aggregatorData = append(a.aggregatorData, v)
	}

	a.handleLeft()
}

//handle left data
func (a *aggregator) handleLeft(){
	leftLen := len(a.aggregatorData)
	if leftLen == 0 {
		return
	}
	//smaller then processCount
	if leftLen < a.processCount{
		a.handler(a.aggregatorData)
		a.aggregatorData = nil
		return
	}

	//bigger then processCount
	gap := len(a.aggregatorData)/a.processCount + 1
	for i:=1;i < gap; i++{
		start := (i-1)*a.processCount
		end := i*a.processCount
		toBeProcess := a.aggregatorData[start:end]
		toBeProcessLen := len(toBeProcess)
		if toBeProcessLen <= 0{
			break
		}
		a.handler(toBeProcess)
		//smaller than a.processCount
		if toBeProcessLen < a.processCount {
			break
		}
	}
	a.aggregatorData = nil

}

