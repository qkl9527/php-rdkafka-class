## php-rdkafka-class
rdkafka的下php-rdkafka的php类库

![kafka version support](https://img.shields.io/badge/kafka-0.8%200.9%201.0%201.1%20or%201.1%2B-brightgreen.svg) 
 
![php version support](https://img.shields.io/badge/php-5.3%2B-green.svg)


![librdkafka version support](https://img.shields.io/badge/librdkafka-3.0.5%2B-yellowgreen.svg)


![php-librdkafka](https://img.shields.io/badge/php--librdkafka-3.0.5%2B-orange.svg)

## 目录

1. [安装](#安装)
3. [使用](#使用)
   * [消费](#消费)
   * [生产](#生产)


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
