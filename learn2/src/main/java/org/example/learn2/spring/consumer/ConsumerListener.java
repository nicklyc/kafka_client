package org.example.learn2.spring.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.learn2.spring.producer.MyproducerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

/**
 * @KafkaLisnter 注解使用方法
 * @author maochao
 * @since 2019/8/7 13:42
 */
public class ConsumerListener {
    private static final Logger logger = LoggerFactory.getLogger(MyproducerListener.class);

    /**[1]:
     *  ConsumerRecord 接收, 可以接收整个record
     * @param record
     */
    @KafkaListener( id="record",        topics = "learn2")
    public void listen(ConsumerRecord record) {
        logger.info("收到消息==>  data: " + record.toString());
    }

    /**
     *[2]: 使用String  只接收  value
     * @param data
     */
    @KafkaListener( id="record2",topics = "learn2")
    public void listen2(String data) {
        logger.info("收到消息==>  data: " + data);
    }


    /**
     * [3] 使用String  只接收  value
     * @param data
     * 使用Spring EL 表达式 重写:container factory 的属性
     *
     */
    @KafkaListener(
            id = "learn",
            topics = "learn2",
            autoStartup = "${listen.auto.start:true}",
            concurrency = "${listen.concurrency:2}"
    )
    public void listen3(String data) {
        logger.info("当先线程；==>" +Thread.currentThread().getName());
        logger.info("收到消息==>  data: " + data);
    }


    /**
     * [4]:
     * 指定分区，指定消费位置
     * @param record
     */
    @KafkaListener(
            id = "bar",
            topicPartitions =
                    { @TopicPartition(topic = "topic1", partitions = { "0", "1" }),
                        @TopicPartition(
                           topic = "topic2",
                           partitions = "0",
                           partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "100")
                        )
                    })
    public void listen4(ConsumerRecord<?, ?> record) {

    }

    /**
     * [5] :参数映射消费
     *
     */
    @KafkaListener(id = "learn", topicPattern = "learn2")
    public void listen5(@Payload String value,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long time
    ){
        logger.info("value : "+value);
        logger.info("topic : "+topic);
    }

    /**
     * [6] 指定 factory
     * @param data
     * @param ack
     */
    @KafkaListener(id = "learn", topics = "learn2",
            containerFactory = "kafkaManualAckListenerContainerFactory")
    public void listen6(String data, Acknowledgment ack) {

    }

    /**
     * [7] 指定 异常处理类
     * @param data
     * @param
     */
    @KafkaListener(id = "learn", topics = "learn2",errorHandler = "consumerErrorHander")
    public void listen7(String data) {

        throw new RuntimeException("消费出错");
    }
}
