package org.example.learn2.spring.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

/**
 * 监听的是template
 * 
 * @author maochao
 * @since 2019/8/7 11:06
 */
public class MyproducerListener implements ProducerListener {
    private static final Logger logger = LoggerFactory.getLogger(MyproducerListener.class);

    @Override
    public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
        logger.info("发送成功" + producerRecord.toString());
    }

    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        logger.info("发送成功 topic:  " + topic + "   value: " + value);
    }

    @Override
    public void onError(ProducerRecord producerRecord, Exception exception) {
        logger.info("发送失败 topic:  " + producerRecord.toString());
    }

    @Override
    public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
        logger.info("发送失败 topic:  " + topic + "   value: " + value);
    }

}
