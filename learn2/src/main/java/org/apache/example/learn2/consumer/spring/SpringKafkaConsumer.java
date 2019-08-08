package org.apache.example.learn2.consumer.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * @author maochao
 * @since 2019/8/6 12:42
 */
public interface SpringKafkaConsumer<T> {

    /**
     * 监听接口
     * @param records
     */
    public void onMessageBatch(List<ConsumerRecord<String, T>> records);

    public void onMessage(ConsumerRecord<String, T> record);
}
