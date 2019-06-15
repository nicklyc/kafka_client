package com.kafka.learn.learn1.quickStart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author
 * @date 2019/5/8
 * 快速开始  生产者
 *
 */
public class QuickStartProducer {
    public static final String url = "192.168.208.128:9092";
    public static final String topic = "quick_start_m";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer =new KafkaProducer<>(initProperties());


        /**
         * 消息是发送到指定的分区，
         *构造消息record的时候，可以通过 Integer partition 指定分区号，指定消息发往的分区.
         * 当没有指定分区号的时候，发送的时候是经过分区器处理，进行的分区
         * 分区器接口  Partitioner：
         *  int partition(String topic,  //
         *              Object key,
         *              byte[] keyBytes, //序列化后的key
         *              Object value,
         *              byte[] valueBytes, //序列化后的value
         *              Cluster cluster);  //集群元数据
         *  默认分区器：DefaultPartitioner
         *     当keyBytes 不为null时候
         *      进行了一系列计算，使用总数区模
         *     Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;numPartitions，该topic的总分区
         *     当keyBytes 为null时候 ，从可用分区中取出一个
         *          availablePartitions.get(part).partition()
         *
         *
         * 看了默认的分区器的实现：可以自定义一个分区器：
         *     只需要实现Partitioner 接口重写分区方法
         *     在配置中配置：
         *     properties.put("partitioner.class"，”自定义类名"),"partitioner.class"=ProducerConfig.PARTITIONER_CLASS_CONFIG
         *
         * 可以根据业务的key，进行分区，
         *
         *
         *
         *
         *
         */
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hellotest","hellotest");
        /**
         * 消息的发送核心方法，Future<RecordMetadata> send(ProducerRecord<K, V> record,
         *                                       Callback callback)
         *  1: send方法返回一个RecordMetadata，消息的元数据，对消息的描述，具体封装数据查看RecordMetadata源码
         *
         *  2: send 方法：本身是异步的，如果调用future.get获取消息的元数据，导致future阻塞，间接导致同步发送消息，
         *     调用send方法，会立即返回，消息将会存在一个缓冲区等待发送，当收到ack回执，则调用callback
         *
         *  3：callback中一方面可以确定，消息的发送成功，一方面可以捕获发送异常，进行业务处理，
         *  callback 中建议使用线程池处理，防止阻塞其他线程发送消息。
         *
         *  4:可以通过 在配置中配置 消息失败的重试次数num：properties.put("retries",num)
         *  retries在ProducerConfig.RETRIES_CONFIG
         *
         *  5:Serializer 可以自定义，继承Serializer ，重新相关方法，在配置中使用自己就的类
         *  properties.put("key.serializer"，”自定义类名")
         *
         */
        //调用的核心方法，send(record,null);
        producer.send(record);

        producer.flush();
        producer.close();
    }




    /**
     * 将配置提取，在kafka中，，KafkaProducer 需要的配置 ProducerConfig 都有常量定义
     * @return
     */
    public static Properties initProperties(){

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        return properties;
    };
}
