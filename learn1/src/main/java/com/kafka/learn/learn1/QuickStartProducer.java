/*
package com.kafka.learn.learn1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

*/
/**
 * @author
 * @date 2019/5/8
 * 快速开始  生产者
 *
 *//*

public class QuickStartProducer {

    */
/**
     * 1：kafka 的生产者生产消息，
     *   造一个消息体 ProducerRecord实例
     *   使用KafkaProducer 的实例进行producer.send(record)，完成了消息的发送
     * 2：ProducerRecord 实例的构造，topic，就是消息的内容相关
     * 3：使用KafkaProducer 初始化，需要 序列化对象，kafka地址
     * KafkaProducer 线程安全。
     * 4：KafkaProducer 构造方法，除config 其他可以为null，
     * 也就是序列化可以通过构造注入
     * KafkaProducer(ProducerConfig config,
     *                   Serializer<K> keySerializer,
     *                   Serializer<V> valueSerializer,
     *                   Metadata metadata,
     *                   KafkaClient kafkaClient)
     * 5：ProducerRecord 的构造：
     *  参考ProducerRecord 的核心构造方法，每个参数的含义，查看核心构造方法源码
     *  ProducerRecord(String topic,
     *                 Integer partition,
     *                 Long timestamp,
     *                 K key,
     *                 V value,
     *                 Iterable<Header> headers)
     * * @param args
     *//*

    public static void main(String[] args) {

        String url = "39.100.104.199:9092";
        String topic = "quick_start";
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", url);

        KafkaProducer<String, String> producer =new KafkaProducer<>(properties);
        StringSerializer stringSerializer = new StringSerializer();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic,"hello_kafka","hello_kafka");
        producer.send(record);
        producer.send(record ,(metadata,e)->{

        });

        producer.flush();
        producer.close();
    }


}
*/
