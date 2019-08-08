package org.apache.example.learn2.consumer.spring;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author maochao
 * @since 2019/8/6 12:48
 */

public class AbstractConsumer<T> implements SpringKafkaConsumer<T> {

    @Override
    public void onMessageBatch(List<ConsumerRecord<String, T>> consumerRecords) {

    }

    @Override
    public void onMessage(ConsumerRecord<String, T> record) {

    }
}
