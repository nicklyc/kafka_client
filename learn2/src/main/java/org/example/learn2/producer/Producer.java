package org.example.learn2.producer;

import org.example.LearnApplication;
import org.example.learn2.entity.KafkaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 生产者
 *
 * @author
 * @date 2019/6/15
 */
//@SpringBootApplication
//@ComponentScan("org")
public class Producer {
    public static void main(String[] args) throws InterruptedException {
       // SpringApplication.run(LearnApplication.class, args);

        KafkaProducer<String, String> producer = new KafkaProducer(ProducerConf.initProperties());
        KafkaMessage kafkaMessage = new KafkaMessage();
        kafkaMessage.setId("测试id");
        KafkaMessage.Message message = kafkaMessage.builder();
        message.setValue(99999L);

        ProducerRecord<String, String> record = new ProducerRecord(ProducerConf.topic, "hellotest", kafkaMessage);

      while (true) {
            producer.send(record, (metadata, exception) -> {
                System.out.println("exception ==》" + exception);
                System.out.println("metadata ==》" + metadata);
            });
            producer.flush();
            Thread.sleep(2000);
           // producer.close();
        }


    }

}
