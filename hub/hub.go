package hub

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gogo/protobuf/proto"
)

var loghub *Loghub

type Config struct {
	LogProject struct {
		Name            string
		Endpoint        string
		AccessKeyID     string
		AccessKeySecret string
	}
	MessageChannelBufferSize int
	LogsBufferSize4Logstore  int
	LogsBufferSize           int
	Logstores                []string
	Topics                   []string

	// http
	MaxIdleConnsPerHost int
}

type Loghub struct {
	*Config
	consumer     *cluster.Consumer
	logproject   *sls.LogProject
	logstoresMap map[string]*sls.LogStore

	m                        sync.RWMutex
	messageChannelBufferSize int
	messages                 chan *sarama.ConsumerMessage

	stop chan int

	mlogstoreLogsBuffer     sync.RWMutex
	logsBuffer4Logstore     map[string](chan *topicLog) // by logstore
	logsBufferSize4Logstore int

	mlogsBuffer    sync.RWMutex
	logsBuffer     map[string]map[string]chan *topicLog // by logstore and topic
	logsBufferSize int
}

type topicLog struct {
	topic string
	log   *sls.Log
	cmsg  *sarama.ConsumerMessage
}

func New(cfg *Config, consumer *cluster.Consumer) *Loghub {
	// 设置MaxIdleConnsPerHost
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	if cfg.MaxIdleConnsPerHost <= 0 {
		http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 10
	}

	logproject := &sls.LogProject{
		Name:            cfg.LogProject.Name,
		Endpoint:        cfg.LogProject.Endpoint,
		AccessKeyID:     cfg.LogProject.AccessKeyID,
		AccessKeySecret: cfg.LogProject.AccessKeySecret,
	}

	lbls := map[string](chan *topicLog){}

	lbr := map[string]map[string](chan *topicLog){}

	lh := &Loghub{
		Config:       cfg,
		consumer:     consumer,
		logproject:   logproject,
		logstoresMap: map[string]*sls.LogStore{},

		messages:                 make(chan *sarama.ConsumerMessage, cfg.MessageChannelBufferSize),
		messageChannelBufferSize: cfg.MessageChannelBufferSize,

		stop: make(chan int),

		logsBuffer4Logstore:     lbls,
		logsBufferSize4Logstore: cfg.LogsBufferSize4Logstore,

		logsBuffer:     lbr,
		logsBufferSize: cfg.LogsBufferSize,
	}

	return lh
}

func (l *Loghub) Run() {
	// 分配消息
	go l.dispatch()

	// lss, err := l.logproject.ListLogStore()
	// if err != nil {
	// 	panic(err)
	// }

	// 开启日志库
	for _, lsn := range l.Logstores {
		_, err := l.getLogstore(lsn)
		if err != nil {
			fmt.Printf("Loghub Start failed (logstoreName=%s, logprojectName=%s). err: %v\n", lsn, l.LogProject.Name, err)
			panic(err)
		}
		// 分配消息到topic
		go l.dispatchToTopic(lsn)
		for _, tp := range l.Topics {
			go l.processTopicMsg(tp, lsn)
		}
	}
}

func (l *Loghub) Stop() {
	l.stop <- 0
}

func (l *Loghub) Input() chan<- *sarama.ConsumerMessage {
	return l.messages
}

// dispatch
// 分配消息
func (l *Loghub) dispatch() error {
	fmt.Printf("============= start dispatch (logproject=%s) ===========\n", l.LogProject.Name)
	// TODO: logproject, logstore, topic
	// 指定logproject和logstore进行分配
	for {
		select {
		case msg := <-l.messages:
			data, err := unserialize(msg)
			if err != nil {
				fmt.Println(err)
				continue
			}
			logprojectName, ok1 := data["logproject"]
			if !ok1 || logprojectName == "" {
				fmt.Println("loghub.dispatch: data[\"logproject\"] was empty")
				continue
			}
			logstoreName, ok2 := data["logstore"]
			if !ok2 || logstoreName == "" {
				fmt.Println("loghub.dispatch: data[\"logstore\"] was empty")
				continue
			}
			topic, ok3 := data["topic"]
			if !ok3 || topic == "" {
				fmt.Println("loghub.dispatch: data[\"topic\"] was empty")
				continue
			}
			log, err := generateLog(data)
			if err != nil {
				fmt.Println(err)
				continue
			}
			// logstore, err := l.getLogstore(logstoreName)
			// if err != nil {
			// 	fmt.Println(err)
			// 	continue
			// }
			log.cmsg = msg
			lblsc := l.getLogstoreLogsBufferChannel(logstoreName)
			select {
			// TODO: 考虑优化处理, lblsc如果满了的情况
			case lblsc <- log:
				fmt.Printf("[dispatch]===========%s\n", msg.Key)
			}
		case <-l.stop:
			return nil
		}
	}
}

// dispatchToTopic
func (l *Loghub) dispatchToTopic(logstoreName string) {
	fmt.Printf("============= start dispatchToTopic (logstoreName=%s) ===========\n", logstoreName)
	var logsCBTopic chan *topicLog
	// TODO: 处理消息, 分配到不同的topic
	channelBuffer := l.getLogstoreLogsBufferChannel(logstoreName)
	for {
		select {
		case log := <-channelBuffer:
			fmt.Printf("[dispatchToTopic]===========logstore=%v, log.topic=%v\n", logstoreName, log.topic)
			if logsCBTopic == nil {
				logsCBTopic = l.getLogsCBTopic(log.topic, logstoreName)
			}
			fmt.Printf("[dispatchToTopic logsCBTopic]==============len=%d,cap=%d\n", len(logsCBTopic), cap(logsCBTopic))
			logsCBTopic <- log
		}
	}
}

func (l *Loghub) getLogsCBTopic(topic string, logstoreName string) chan *topicLog {
	l.mlogsBuffer.Lock()
	defer l.mlogsBuffer.Unlock()
	logsCB, ok := l.logsBuffer[logstoreName]
	if !ok || logsCB == nil {
		logsCB = map[string]chan *topicLog{}
		l.logsBuffer[logstoreName] = logsCB
	}
	logsCBTopic, ok := logsCB[topic]
	if !ok || logsCBTopic == nil {
		logsCBTopic = make(chan *topicLog, l.logsBufferSize)
		logsCB[topic] = logsCBTopic
	}
	return logsCBTopic
}

func (l *Loghub) processTopicMsg(topic string, logstoreName string) error {
	fmt.Printf("============ start processTopicMsg (topic=%s,logstore=%v) ============\n", topic, logstoreName)
	cb := l.getLogsCBTopic(topic, logstoreName)
	fmt.Printf("============ start processTopicMsg (logsTopicChannelBuffer len=%d,cap=%d) ============\n", len(cb), cap(cb))
	for {
		select {
		case log := <-cb:
			fmt.Printf("[processTopicMsg]=================key:%s\n", log.cmsg.Key)
			// TODO: 优化, 累积一定的log记录再提交(附加超时提交策略)
			// generateLoggroupByTopicLog 是否需要定义参数source
			loggroup := generateLoggroupByTopicLog(log, "")
			logstore, err := l.getLogstore(logstoreName)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = putLogs(logstore, loggroup)
			if err != nil {
				fmt.Println(err)
				continue
			}
			l.consumer.MarkOffset(log.cmsg, "loghub.processTopicMsg")
			fmt.Printf("[processTopicMsg success]=================key:%s\n", log.cmsg.Key)
		}
	}
}

func (l *Loghub) getLogstore(logstoreName string) (*sls.LogStore, error) {
	var logstore *sls.LogStore
	l.m.RLock()
	logstore = l.logstoresMap[logstoreName]
	l.m.RUnlock()
	if logstore != nil {
		return logstore, nil
	}
	var err error
	for retry_times := 0; ; retry_times++ {
		if retry_times > 5 {
			return nil, errors.New("GetLogStore retry_times > 5")
		}
		logstore, err = l.logproject.GetLogStore(logstoreName)
		if err != nil {
			fmt.Printf("GetLogStore fail, retry:%d, err:%v\n", retry_times, err)
			if strings.Contains(err.Error(), sls.PROJECT_NOT_EXIST) {
				return nil, err
			} else if strings.Contains(err.Error(), sls.LOGSTORE_NOT_EXIST) {
				err = l.logproject.CreateLogStore(logstoreName, 1, 2)
				if err != nil {
					fmt.Printf("CreateLogStore fail, err: ", err.Error())
					return nil, err
				} else {
					fmt.Println("CreateLogStore success")
					l.m.Lock()
					l.logstoresMap[logstoreName] = logstore
					l.m.Unlock()
					return logstore, nil
				}
			}
		} else {
			fmt.Printf("GetLogStore success, retry:%d, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", retry_times, logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
			l.m.Lock()
			l.logstoresMap[logstoreName] = logstore
			l.m.Unlock()
			return logstore, nil
		}
	}
}

func (l *Loghub) getLogstoreLogsBufferChannel(logstoreName string) chan *topicLog {
	l.mlogstoreLogsBuffer.RLock()
	slslchan, ok := l.logsBuffer4Logstore[logstoreName]
	l.mlogstoreLogsBuffer.RUnlock()
	if !ok || slslchan == nil {
		slslchan = make(chan *topicLog, l.logsBufferSize4Logstore)
		l.mlogstoreLogsBuffer.Lock()
		l.logsBuffer4Logstore[logstoreName] = slslchan
		l.mlogstoreLogsBuffer.Unlock()
	}
	return slslchan
}

func generateLoggroupByTopicLog(tlog *topicLog, source string) *sls.LogGroup {
	logs := []*sls.Log{tlog.log}
	loggroup := &sls.LogGroup{
		Topic:  proto.String(tlog.topic),
		Source: proto.String(source),
		Logs:   logs,
	}
	return loggroup
}

func unserialize(msg *sarama.ConsumerMessage) (map[string]string, error) {
	var err error
	data := map[string]string{}
	err = json.Unmarshal(msg.Value, &data)
	if err != nil {
		fmt.Printf("[unserialize sarama.ConsumerMessage] json.Unmarshal err: %v\n", err)
		return nil, err
	}
	return data, nil
}

func generateLog(data map[string]string) (*topicLog, error) {
	contents := []*sls.LogContent{}
	for k, v := range data {
		contents = append(contents, &sls.LogContent{
			Key:   proto.String(k),
			Value: proto.String(v),
		})
	}
	local, _ := time.LoadLocation("Local")
	t, e := time.ParseInLocation("2006-01-02T15:04:05+08:00", data["@timestamp"], local)
	if e != nil {
		t = time.Now()
	}
	log := &sls.Log{
		Time:     proto.Uint32(uint32(t.Unix())),
		Contents: contents,
	}
	tplog := &topicLog{
		topic: data["topic"],
		log:   log,
	}
	return tplog, nil
}

func putLogs(logstore *sls.LogStore, loggroup *sls.LogGroup) error {
	var retry_times int
	var err error
	// PostLogStoreLogs API Ref: https://intl.aliyun.com/help/doc-detail/29026.htm
	for retry_times = 0; retry_times < 10; retry_times++ {
		err = logstore.PutLogs(loggroup)
		if err == nil {
			fmt.Printf("PutLogs success, retry: %d\n", retry_times)
			return nil
		}
		fmt.Printf("PutLogs fail, retry: %d, err: %s\n", retry_times, err)
		//handle exception here, you can add retryable erorrCode, set appropriate put_retry
		if strings.Contains(err.Error(), sls.WRITE_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.PROJECT_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.SHARD_WRITE_QUOTA_EXCEED) {
			//you should split shard
		} else if strings.Contains(err.Error(), sls.INTERNAL_SERVER_ERROR) || strings.Contains(err.Error(), sls.SERVER_BUSY) {
		}
		continue
	}
	return err
}
