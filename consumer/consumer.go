package consume

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"loghub/hub"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	configs "loghub/configs"
)

var sig chan os.Signal
var consumer *cluster.Consumer

func init() {
	sig = make(chan os.Signal, 1)
}

func New(mqcfg *configs.MqConfig) *cluster.Consumer {
	clusterCfg := cluster.NewConfig()

	// TODO: 优化net配置
	clusterCfg.Net.KeepAlive = 60 * time.Second
	clusterCfg.Net.SASL.Enable = true
	clusterCfg.Net.SASL.User = mqcfg.Ak
	clusterCfg.Net.SASL.Password = mqcfg.Password
	clusterCfg.Net.SASL.Handshake = true

	certBytes, err := ioutil.ReadFile(configs.GetFullPath(mqcfg.CertFile))
	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		panic("kafka consumer failed to parse root certificate")
	}

	clusterCfg.Net.TLS.Config = &tls.Config{
		//Certificates:       []tls.Certificate{},
		RootCAs:            clientCertPool,
		InsecureSkipVerify: true,
	}

	clusterCfg.Net.TLS.Enable = true
	// TODO: 优化获取策略, 提高吞吐量, clusterCfg.Consumer.Fetch
	// clusterCfg.Consumer.Fetch.Default
	clusterCfg.Consumer.Return.Errors = true
	clusterCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusterCfg.Group.Return.Notifications = true

	clusterCfg.ChannelBufferSize = 1024

	clusterCfg.Version = sarama.V0_10_0_0
	if err = clusterCfg.Validate(); err != nil {
		msg := fmt.Sprintf("Kafka consumer config invalidate. config: %v. err: %v", *clusterCfg, err)
		fmt.Println(msg)
		panic(msg)
	}

	consumer, err = cluster.NewConsumer(mqcfg.Servers, mqcfg.ConsumerId, mqcfg.Topics, clusterCfg)
	if err != nil {
		msg := fmt.Sprintf("Create kafka consumer error: %v. config: %v", err, clusterCfg)
		fmt.Println(msg)
		panic(msg)
	}
	return consumer
}

func Start(loghub *hub.Loghub) {
	go consume(loghub)
}

func consume(loghub *hub.Loghub) {
	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				// fmt.Printf("kafka consumer msg: (topic:%s) (partition:%d) (offset:%d) (%s): (%s)\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				loghub.Input() <- msg
				// consumer.MarkOffset(msg, "completed") // mark message as processed
				// fmt.Println("kafka consumer HighWaterMarks", consumer.HighWaterMarks())
			}
		case err, more := <-consumer.Errors():
			if more {
				fmt.Printf("Kafka consumer error: %v\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				fmt.Printf("Kafka consumer rebalance: %v\n", ntf)
			}
		case <-sig:
			fmt.Errorf("Stop consumer server...")
			consumer.Close()
			return
		}
	}

}

func Stop(s os.Signal) {
	fmt.Println("Recived kafka consumer stop signal...")
	sig <- s
	fmt.Println("kafka consumer stopped!!!")
}
