<?php
/**
 * Created by PhpStorm.
 * User: qkl
 * Date: 2018/8/14
 * Time: 15:45
 */
namespace Vendors\Queue\Msg\Kafka;

class Producer
{
    private $producer;
    private $producerTopic;
    private $producerTopicConf;

    public function __construct($config = [])
    {
        $this->rk = new Rdkafka($config);
        $this->rkConf = $this->rk->getConf();
        $this->config = $this->rk->getConfig();
        $this->brokerConfig = $this->rk->getBrokerConfig();
    }

    public function setBrokerServer($ip = NULL)
    {
        $this->producer = new \RdKafka\Producer($this->rkConf);
        $this->producer->setLogLevel(LOG_DEBUG);
        $this->producer->addBrokers($ip ? : $this->config['ip']);

        return $this;
    }

    public function setProducerTopic($topicName)
    {
        $this->producerTopicConf = new \RdKafka\TopicConf();
        // -1必须等所有brokers确认 1当前服务器确认 0不确认，这里如果是0回调里的offset无返回，如果是1和-1会返回offset
        $this->producerTopicConf->set('request.required.acks', $this->brokerConfig['request.required.acks']);
        $this->producerTopic = $this->producer->newTopic($topicName, $this->producerTopicConf);

        return $this;
    }

    public function producer($msg, $option)
    {
        $this->producerTopic->produce(RD_KAFKA_PARTITION_UA, 0, $msg, $option);

        $len = $this->producer->getOutQLen();
        while ($len > 0) {
            $len = $this->producer->getOutQLen();
            $this->producer->poll(50);
        }

        return true;
    }
}