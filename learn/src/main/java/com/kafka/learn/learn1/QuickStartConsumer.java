package com.kafka.learn.learn1;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * @author
 * @date 2019/5/8
 * 快速开始 消费者
 *
 */
public class QuickStartConsumer {



    /**1：KafkaConsumer 初始化，然后订阅consumer.subscribe(list) 订阅多个topic
     * 2:consumer.poll  定时拉取消息就行，则完成了消息的消费
     * KafkaConsumer的初始化，需要的是一组配置
     *
     * @param args
     */
    public static void main(String[] args) {

        String url = "39.100.104.199:9092";
        String topic = "quick_start";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", url);
        properties.put("group.id", UUID.randomUUID().toString());
         properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s," +
                                "offset = %d, " +
                                "key = %s, " +
                                "partition = %s, " +
                                "value = %s%n",
                        record.topic(),
                        record.offset(),
                        record.key(),
                        record.partition(),
                        record.value());

            }
        }
    }
}
