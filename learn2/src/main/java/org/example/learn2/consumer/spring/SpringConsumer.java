package org.example.learn2.consumer.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author maochao
 * @since 2019/8/6 12:45
 */
@Service
public class SpringConsumer extends AbstractConsumer <String> {


    @Override

    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println(record);
    }

    @KafkaListener(
            topics = "learn2",
            groupId = "learn")
    public void listen(Object data) {
        System.out.println(data);
    }

}
