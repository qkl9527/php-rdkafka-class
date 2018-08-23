## php-rdkafka-class
rdkafka的下php-rdkafka的php类库

[![kafka version support](https://img.shields.io/badge/kafka-0.8%200.9%201.0%201.1%20or%201.1%2B-brightgreen.svg)](#) [![php version support](https://img.shields.io/badge/php-5.3%2B-green.svg)](#) [![librdkafka version support](https://img.shields.io/badge/librdkafka-3.0.5%2B-yellowgreen.svg)](#) [![php-librdkafka](https://img.shields.io/badge/php--librdkafka-3.0.5%2B-orange.svg)](#)

## 目录

1. [安装](#安装)
2. [使用](#使用)
   * [消费](#消费)
   * [生产](#生产)
3. [更多配置](#更多配置)

## 安装
> 具体查看librdkafk和php-rdkafka

## 使用
### 消费
```
# setTopic('qkl01', 0, $offset)  不设置，从最后一次服务器记录一次消费开始消费
$offset = 86; //开始消费点
$consumer = new \Vendors\Queue\Msg\Kafka\Consumer(['ip'=>'192.168.216.122']);
$consumer->setConsumerGroup('test-110-sxx1')
     ->setBrokerServer('192.168.216.122')
     ->setConsumerTopic()
     ->subscribe('qkl01')
     ->setTopic('qkl01', 0, $offset)
     ->subscribe(['qkl01'])
     ->consumer(function($msg){
         var_dump($msg);
     });
```


### 生产
```
$config = [
    'ip'=>'192.168.216.122',
    'dr_msg_cb' => function($kafka, $message) {
        var_dump((array)$message);
        //todo
        //do biz something, don't exit() or die()
    }
];
$producer = new \Vendors\Queue\Msg\Kafka\Producer($config);
$rst = $producer->setBrokerServer()
                 ->setProducerTopic('qkl01')
                 ->producer('qkl037', 90);

var_dump($rst);
```

## 更多配置
```
$defaultConfig = [
    'ip'=>'127.0.0.1',  #默认服务器地址
    'log_path'=> sys_get_temp_dir(),  #日志默认地址
    'dr_msg_cb' => [$this, 'defaultDrMsg'],  #生产的dr回调
    'error_cb' => [$this, 'defaultErrorCb'],  #错误回调
    'rebalance_cb' => [$this, 'defaultRebalance']  #负载回调，你可以用匿名方法自定义
];

# broker相关配置，你可以参考Configuration.md
$brokerConfig = [
    'request.required.acks'=> -1,
    'auto.commit.enable'=> 1,
    'auto.commit.interval.ms'=> 100,
    'offset.store.method'=> 'broker',
    'offset.store.path'=> sys_get_temp_dir(),
    'auto.offset.reset'=> 'smallest',
];
```

### defaultDrMsg
```
function defaultDrMsg($kafka, $message) {
    file_put_contents($this->config['log_path'] . "/dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
}
```

### defaultErrorCb
```
function defaultErrorCb($kafka, $err, $reason) {
    file_put_contents($this->config['log_path'] . "/err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
}
```


### defaultRebalance
```
function defaultRebalance(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
{
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "Assign: ";
            if (is_null($this->getCurrentTopic())) {
                $kafka->assign();
            } else {
                $kafka->assign([
                    new \RdKafka\TopicPartition( $this->getCurrentTopic(), $this->getPartition($this->getCurrentTopic()), $this->getOffset($this->getCurrentTopic()) )
                ]);
            }
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "Revoke: ";
            var_dump($partitions);
            $kafka->assign(NULL);
            break;

        default:
            throw new \Exception($err);
    }
}
```
