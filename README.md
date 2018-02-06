# loghub

--------

集成阿里云日志服务sdk, 阿里云kafka消息消费者

## 支持

--------

- 单消费者, 多topic
- 单logproject, 多logstore

## 计划支持

--------

- 优化, 日志记录提交缓冲, 定时提交
- 多消费者, 多logproject
- 根据kafka消息, 动态创建logproject, logstore
- 支持默认的logproject, logstore

## 运行

--------

```bash
go run main.go -kafkaAK xxx -kafkaPassword xxx -kafkaConfigPath mq.test.json -logaccesskeyid xxx -logaccesskeysecret xxx -logstore gateway -logproject epaper -logendpoint cn-beijing.log.aliyuncs.com
```