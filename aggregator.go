package aggregator

import (
	"time"
	"fmt"
	"sync"
	"sync/atomic"
)


import (
	"github.com/baidu/go-lib/log"
)


const TickTime = time.Millisecond * 100

//aggregation to deal
type aggregator struct{
	wait *sync.WaitGroup
	handler func([] interface{})error
	errorHandler func(msg string)
	aggregaterData []interface{}
	processCount int //process count
	lock *sync.Mutex
	workCount int
	dataChan chan interface{}
	stopped chan struct{}
	isStop int32
}


func NewAggregater(workCount, chanSize, processCount int )*aggregator{
	return &aggregator{
		wait:&sync.WaitGroup{},
		workCount:workCount,
		processCount:processCount,
		dataChan: make(chan interface{}, chanSize),
		stopped: make(chan struct{}),
		isStop: 0,
		aggregaterData:make([]interface{},0),
		lock: &sync.Mutex{},
	}
}

func (a *aggregator)SetHandler(handler func([] interface{})error){
	a.handler = handler
}

func (a *aggregator)SetErrorHandler(errorHandler func(msg string)){
	a.errorHandler = errorHandler
}

func (a *aggregator)Receive(data interface{}){
	//not close
	if a.IsStopped(){
		return
	}
	a.dataChan<-data
}

//simple pool
func (a *aggregator) Start(){

	for i:=0; i < a.workCount; i++ {
		log.Logger.Info("start aggregater worker :%d",i)
		go func(){
			a.wait.Add(1)
			worker(a)
		}()
	}
}

func worker(a *aggregator){
	defer a.wait.Done()



	defer func(){

		if err := recover(); err != nil {
			if a.errorHandler != nil {
				a.errorHandler(fmt.Sprintf("aggregaterData has not deal %d", len(a.aggregaterData)))
			}
			log.Logger.Error("worker error  %+v", err)
		}
	}()

	ticker := time.NewTicker(TickTime)
	var toBeProcess []interface{}

loop:
	for{
		select {
		case <-a.stopped:
			a.addData(toBeProcess)
			a.StopAggregator()
			break loop
		case data,ok := <- a.dataChan:
			if a.IsStopped() || !ok{
				a.addData(toBeProcess)
				a.StopAggregator()
				break loop
			}else{
				toBeProcess = append(toBeProcess, data)
				log.Logger.Info("aggregate Data append data toBeProcess len = %d", len(toBeProcess))
				if len(toBeProcess) >= a.processCount {
					err := a.handler(toBeProcess)
					if err == nil {
						toBeProcess = make([]interface{}, 0)
					}else {
						log.Logger.Error("aggregate handler msg failed %+v",toBeProcess)
					}
				}else {
					log.Logger.Info(" %d is smaller than %d ", len(toBeProcess), a.processCount)
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
	log.Logger.Info("addData toBeProcess length =%d", len(toBeProcess))
	if len(toBeProcess) > 0{
		a.lock.Lock()
		a.aggregaterData = append(a.aggregaterData, toBeProcess...)
		a.lock.Unlock()
	}
	log.Logger.Info("addData addData stopped aggregate Data length =%d", len(a.aggregaterData))

}


func (a *aggregator)StopAggregator(){
	atomic.StoreInt32(&(a.isStop),1)
}

func (a *aggregator)IsStopped()bool{
	return atomic.LoadInt32(&(a.isStop)) == 1
}

func (a *aggregator) Stop(){
	a.stopped <- struct{}{}
	close(a.dataChan)
	a.wait.Wait()

	log.Logger.Info("aggregate.stop=%d", len(a.aggregaterData))

	//range data chan
	for v := range a.dataChan{
		a.aggregaterData = append(a.aggregaterData, v)
	}

	a.handleLeft()
}


func (a *aggregator) handleLeft(){
	leftLen := len(a.aggregaterData)

	log.Logger.Info("handleLeft length=%d", leftLen)

	if leftLen == 0 {
		log.Logger.Info("aggregate Data is empty")
		return
	}
	log.Logger.Info("aggregate left %+v", a.aggregaterData)

	//smaller then processCount
	if leftLen < a.processCount{
		log.Logger.Info("aggregate handle left")
		a.handler(a.aggregaterData)
		a.aggregaterData = nil
		return
	}

	//bigger then processCount
	gap := len(a.aggregaterData)/a.processCount + 1
	for i:=1;i < gap; i++{
		start := (i-1)*a.processCount
		end := i*a.processCount
		toBeProcess := a.aggregaterData[start:end]
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
	a.aggregaterData = nil
}






















