package main

import (
	"flag"
	"fmt"
	"os"

	configs "loghub/configs"
	"loghub/consumer"
	"loghub/hub"
	"os/signal"
)

var loghub *hub.Loghub

func init() {
	fmt.Println("============ init... ============")

	// loghub
	lhcfg := &hub.Config{
		MessageChannelBufferSize: 256,
		LogsBufferSize4Logstore:  10,
		LogsBufferSize:           10,
	}
	flag.StringVar(&lhcfg.LogProject.Name, "logproject", "epaper", "loghub project")
	// cn-beijing-intranet.log.aliyuncs.com
	// cn-beijing.log.aliyuncs.com
	flag.StringVar(&lhcfg.LogProject.Endpoint, "logendpoint", "cn-beijing.log.aliyuncs.com", "loghub endpoint")
	flag.StringVar(&lhcfg.LogProject.AccessKeyID, "logaccesskeyid", "", "loghub AccessKeyID")
	flag.StringVar(&lhcfg.LogProject.AccessKeySecret, "logaccesskeysecret", "", "loghub AccessKeySecret")

	var logstore string
	flag.StringVar(&logstore, "logstore", "gateway", "log store")

	// mq
	mqcfg := &configs.MqConfig{}
	var kafkaConfigPath string
	var ak string
	var pwd string
	flag.StringVar(&kafkaConfigPath, "kafkaConfigPath", "mq.json", "kafka config path")
	flag.StringVar(&ak, "kafkaAK", "", "kafka access key")
	flag.StringVar(&pwd, "kafkaPassword", "", "kafka access password")
	flag.Parse()

	configs.LoadJsonConfig(mqcfg, kafkaConfigPath)
	if ak != "" {
		mqcfg.Ak = ak
	}
	if pwd != "" {
		mqcfg.Password = pwd
	}

	lhcfg.Logstores = []string{logstore}
	lhcfg.Topics = mqcfg.Topics

	fmt.Printf("============ load mqcfg: %+v ============\n", mqcfg)
	fmt.Printf("============ load lhcfg: %+v ============\n", lhcfg)

	consumer := consume.New(mqcfg)
	loghub = hub.New(lhcfg, consumer)
	loghub.Run()
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consume.Start(loghub)

	select {
	case s := <-signals:
		consume.Stop(s)
	}
}
