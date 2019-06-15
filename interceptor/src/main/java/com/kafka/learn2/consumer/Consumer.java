package com.kafka.learn2.consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * @author
 * @date 2019/5/8
 * 快速开始 消费者
 *
 */
public class Consumer {

    public static final String url = "192.168.208.128:9092";
    public static final String topic = "quick_start_m";

    public static void main(String[] args) {
        Properties properties = initProperties();
       //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        /**订阅topic
         *     subscribe(Collections.singletonList(topic))  //订阅一组topic
         *     subscribe(Pattern pattern)  //订阅一组正则匹配的topics，
         *
         *  可以指定订阅topic的特定分区
         *  assign(Collection<TopicPartition> partitions)
         *  TopicPartition封装了topic,和分区号 。
         *     private final int partition;
         *     private final String topic;
         * 用来指定某一个topic的部分分区。
         *
         * 总结：
         *    1:consumer提供了两个api对队列进行订阅，subscribe，assign
         *    2：subscribe,可以进行接受list,和正则，进行多个订阅。
         *    3：assign，订阅一组指定分区的topic,
         *    4：subscribe API中提供了ConsumerRebalanceListener，具有负载均衡功能监听器。
         *    当消费者的数量发生变化的时候，可以为消费者，动态的重新分配分区，实现负载均衡和故障转移，
         *    但是assign，由于指定了特定的分区，所以不再具有负载功能。
         *
         *    5：subscribe,和assign方法，多次使用会覆盖。不会叠加，
         *    6：subscribe 对应的unsubscribe api ，可以对订阅进行取消
         *
         *consumer还提供了一个查询某个topic的分区元数据信息api
         *   List<PartitionInfo> partitionsFor(String topic, Duration timeout)
         *   查询的是指定的topic的分区数据
         *   PartitionInfo 就是封装的一个分区的数据，
         *
         */
        consumer.subscribe(Collections.singletonList(topic));
        //拉取一条消息
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("topic = %s,offset = %d, key = %s, value = %s%n",record.topic(), record.offset(), record.key(), record.value());
            }

    }




    /**
     * 将配置提取，在kafka中，，KafkaProducer 需要的配置 ProducerConfig 都有常量定义
     * @return
     */
    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    };
}
