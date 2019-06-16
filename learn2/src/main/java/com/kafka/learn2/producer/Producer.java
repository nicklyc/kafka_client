package com.kafka.learn2.producer;

import com.kafka.learn2.entity.KafkaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Properties;

/**
 * 生产者
 *
 * @author
 * @date 2019/6/15
 */
public class Producer {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer(ProducerConf.initProperties());
        KafkaMessage message = new KafkaMessage();
        message.setId("123");
        message.setValue(12123L);
        ProducerRecord<String, String> record = new ProducerRecord(ProducerConf.topic, "hellotest", message);

        while (true) {
            producer.send(record, (metadata, exception) -> {
                System.out.println("exception ==》" + exception);
            });
            producer.flush();
            Thread.sleep(2000);
           // producer.close();
        }


    }

}
