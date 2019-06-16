package com.kafka.learn2.consumer;

import com.kafka.learn2.entity.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Collections;
import java.util.Iterator;

/**
 * 消费者
 *
 * @author
 * @date 2019/6/15
 */
public class Consumer {

    public static void main(String[] args) {
        KafkaConsumer<String, Object> consumer = new KafkaConsumer(ConsumerConf.initProperties());
        consumer.subscribe(Collections.singletonList(ConsumerConf.topic));
        while (true) {
            System.out.println("拉取消息一次");
            ConsumerRecords<String, Object> records = consumer.poll(1000);
            handleMess(records);
        }

    }

    /**
     * 处理消息
     *
     * @param records
     */
    private static void handleMess(ConsumerRecords<String, Object> records) {
        for (ConsumerRecord<String, Object> record : records) {
            if(record.value() instanceof KafkaMessage){
                System.out.println("value = "+((KafkaMessage) record.value()).getId());
            }

            System.out.printf("topic = %s, key = %s", record.topic(), record.key());
            /**
             * 消息头
             */
            RecordHeaders headers = (RecordHeaders) record.headers();
            headers.forEach(header -> {
                System.out.println("key=" + header.key() + "value=" + new String(header.value()));
            });
        }
    }
}
