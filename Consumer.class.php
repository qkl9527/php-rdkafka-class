<?php
/**
 * Created by PhpStorm.
 * User: qkl
 * Date: 2018/8/14
 * Time: 15:45
 */
namespace Vendors\Queue\Msg\Kafka;

class Consumer
{
    private $consumer;
    private $consumerTopic;

    public function __construct($config = [])
    {
        $this->rk = new Rdkafka($config);
        $this->rkConf = $this->rk->getConf();
        $this->config = $this->rk->getConfig();
        $this->brokerConfig = $this->rk->getBrokerConfig();
    }

    /**
     * 设置消费组
     * @param $groupName
     */
    public function setConsumerGroup($groupName)
    {
        $this->rkConf->set('group.id', $groupName);
        return $this;
    }

    /**
     * 设置服务broker
     * $broker: 127.0.0.1|127.0.0.1:9092|127.0.0.1:9092,127.0.0.1:9093
     * @param $groupName
     */
    public function setBrokerServer($broker)
    {
        $this->rkConf->set('metadata.broker.list', $broker);
        return $this;
    }

    /**
     * 设置服务broker
     * $broker: 127.0.0.1|127.0.0.1:9092|127.0.0.1:9092,127.0.0.1:9093
     * @param $groupName
     */
    public function setTopic($topicName, $partition = 0, $offset = 0)
    {
        $this->rk->setTopic($topicName, $partition, $offset);
        return $this;
    }

    public function setConsumerTopic()
    {
        $this->topicConf = new \RdKafka\TopicConf();

        $this->topicConf->set('request.required.acks', $this->brokerConfig['request.required.acks']);
        //在interval.ms的时间内自动提交确认、建议不要启动
        $this->topicConf->set('auto.commit.enable', $this->brokerConfig['auto.commit.enable']);
        if ($this->brokerConfig['auto.commit.enable']) {
            $this->topicConf->set('auto.commit.interval.ms', $this->brokerConfig['auto.commit.interval.ms']);
        }

        // 设置offset的存储为file
//        $this->topicConf->set('offset.store.method', 'file');
//        $this->topicConf->set('offset.store.path', __DIR__);
        // 设置offset的存储为broker
        // $this->topicConf->set('offset.store.method', 'broker');
         $this->topicConf->set('offset.store.method', $this->brokerConfig['offset.store.method']);
        if ($this->brokerConfig['offset.store.method'] == 'file') {
            $this->topicConf->set('offset.store.path', $this->brokerConfig['offset.store.path']);
        }

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $this->topicConf->set('auto.offset.reset', 'smallest');
        $this->topicConf->set('auto.offset.reset', $this->brokerConfig['auto.offset.reset']);

        //设置默认话题配置
        $this->rkConf->setDefaultTopicConf($this->topicConf);

        return $this;
    }

    public function getConsumerTopic()
    {
        return $this->topicConf;
    }

    public function subscribe($topicNames)
    {
        $this->consumer = new \RdKafka\KafkaConsumer($this->rkConf);
        $this->consumer->subscribe($topicNames);
        return $this;
    }

    public function consumer(\Closure $handle)
    {
        while (true) {
            $message = $this->consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $handle($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    public function consumer2(\Closure $callback)
    {
        //参数1表示消费分区，这里是分区0
        //参数2表示同步阻塞多久
        $message = $this->consumerTopic->consume(0, 12 * 1000);
        var_dump($message);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                //todo 消费
                $callback($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                echo $message->err . ":" . $message->errstr;
//                throw new \Exception($message->errstr(), $message->err);
                break;
        }
//        while (true) {
//            //参数1表示消费分区，这里是分区0
//            //参数2表示同步阻塞多久
//            $message = $this->consumerTopic->consume(0, 12 * 1000);
//            switch ($message->err) {
//                case RD_KAFKA_RESP_ERR_NO_ERROR:
//                    //todo 消费
//                    $callback($message);
//                    break;
//                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
//                    echo "No more messages; will wait for more\n";
//                    break;
//                case RD_KAFKA_RESP_ERR__TIMED_OUT:
//                    echo "Timed out\n";
//                    break;
//                default:
//                    throw new \Exception($message->errstr(), $message->err);
//                    break;
//            }
//        }
    }
}