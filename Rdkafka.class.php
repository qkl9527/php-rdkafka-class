<?php
/**
 * Created by PhpStorm.
 * User: qkl
 * Date: 2018/8/14
 * Time: 15:45
 */
namespace Vendors\Queue\Msg\Kafka;

use Think\Log;

class Rdkafka
{
    private $rkConf;
    private $config = [];

    public function __construct($config = [])
    {
        $defaultConfig = [
            'ip'=>'127.0.0.1',
            'log_path'=> sys_get_temp_dir(),
            'dr_msg_cb' => [$this, 'defaultDrMsg'],
            'error_cb' => [$this, 'defaultErrorCb'],
            'rebalance_cb' => [$this, 'defaultRebalance']
        ];

        $brokerConfig = [
            'request.required.acks'=> -1,
            'auto.commit.enable'=> 1,
            'auto.commit.interval.ms'=> 100,
            'offset.store.method'=> 'broker',
            'offset.store.path'=> sys_get_temp_dir(),
            'auto.offset.reset'=> 'smallest',
        ];

        $this->config = array_merge($defaultConfig, $config);
        $this->brokerConfig = array_merge($brokerConfig, isset($config['broker']) ? $config['broker'] : []);

        $this->rkConf = new \RdKafka\Conf();

        $this->setDrMsgCb($this->config['dr_msg_cb']);
        $this->setErrorCb($this->config['error_cb']);
        $this->setRebalanceCb($this->config['rebalance_cb']);
    }

    public function getConf()
    {
        return $this->rkConf;
    }

    public function getConfig()
    {
        return $this->config;
    }

    public function getBrokerConfig()
    {
        return $this->brokerConfig;
    }

    /**
     * 设置话题、partition、offset
     * @param $topicName
     * @param int $offset
     */
    public function setTopic($topicName, $partition = 0, $offset = 0)
    {
        $this->topics[$topicName] = $topicName;
        $this->partitions[$topicName] = $partition;
        $this->offsets[$topicName] = $offset;

        $this->topic = $topicName;
    }

    /**
     * 获取话题
     * @return mixed
     */
    public function getCurrentTopic()
    {
        return $this->topic;
    }

    /**
     * 获取话题的offset
     * @param $topicName
     * @return mixed
     */
    public function getPartition($topicName)
    {
        return $this->partitions[$topicName];
    }

    /**
     * 获取话题的offset
     * @param $topicName
     * @return mixed
     */
    public function getOffset($topicName)
    {
        return $this->offsets[$topicName];
    }

    public function setDrMsgCb($callback)
    {
//        $this->rkConf->setDrMsgCb(function ($kafka, $message) {
//            file_put_contents("./dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
//        });

        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setDrMsgCb(function ($kafka, $message) use ($callback) {
            call_user_func_array($callback, [$kafka, $message]);
        });
    }

    public function setErrorCb($callback)
    {
//        $this->rkConf->setErrorCb(function ($kafka, $err, $reason) {
//            file_put_contents("./err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
//        });

        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setErrorCb(function ($kafka, $err, $reason) use ($callback) {
            call_user_func_array($callback, [$kafka, $err, $reason]);
        });
    }

    public function setRebalanceCb($callback)
    {
//        $this->rkConf->setErrorCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) use ($topic){
//            global $offset;
//            switch ($err) {
//                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
//                    echo "Assign: ";
////                    var_dump($partitions);
//                    if (is_null($topic)) {
//                        $kafka->assign();
//                    } else {
//                        $kafka->assign([new \RdKafka\TopicPartition("qkl01", 0, $offset)]);
//                    }
//                    break;
//
//                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
//                    echo "Revoke: ";
//                    var_dump($partitions);
//                    $kafka->assign(NULL);
//                    break;
//
//                default:
//                    throw new \Exception($err);
//            }
//        });


        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setRebalanceCb(function ($kafka, $err, $partitions) use ($callback) {
            call_user_func_array($callback, [$kafka, $err, $partitions]);
        });
    }

    private function defaultDrMsg($kafka, $message) {
        file_put_contents($this->config['log_path'] . "/dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
    }

    private function defaultErrorCb($kafka, $err, $reason) {
        file_put_contents($this->config['log_path'] . "/err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
    }

    private function defaultRebalance(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
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
}