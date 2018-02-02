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
	fmt.Println("init...")

	// loghub
	lhcfg := &hub.Config{
		MessageChannelBufferSize: 256,
		LogsBufferSize4Logstore:  10,
		LogsBufferSize:           10,
	}
	flag.StringVar(&lhcfg.LogProject.Name, "projectname", "epaper", "loghub project name")
	flag.StringVar(&lhcfg.LogProject.Endpoint, "endpoint", "cn-beijing.log.aliyuncs.com", "loghub endpoint")
	flag.StringVar(&lhcfg.LogProject.AccessKeyID, "accesskeyid", "", "loghub AccessKeyID")
	flag.StringVar(&lhcfg.LogProject.AccessKeySecret, "accesskeysecret", "", "loghub AccessKeySecret")

	// mq
	mqcfg := &configs.MqConfig{}
	configs.LoadJsonConfig(mqcfg, "mq.json")
	flag.StringVar(&mqcfg.Ak, "ak", "", "access key")
	flag.StringVar(&mqcfg.Password, "password", "", "password")
	flag.Parse()

	lhcfg.Logstores = []string{"gateway"}
	lhcfg.Topics = mqcfg.Topics

	fmt.Printf("load mqcfg: %+v\n", mqcfg)
	fmt.Printf("load lhcfg: %+v\n", lhcfg)

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
