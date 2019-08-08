package org.example.learn2.spring;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CountDownLatch;

/**
 * 手动提交实现方式
 * 
 * @author maochao
 * @since 2019/8/6 17:06
 */
public class MyAcknowledgingMessageListener implements AcknowledgingMessageListener {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaTest.class);

    private CountDownLatch latch;

    public MyAcknowledgingMessageListener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onMessage(ConsumerRecord data) {

    }

    @Override
    public void onMessage(ConsumerRecord data, Acknowledgment acknowledgment) {
        logger.info("收到消息===>"+data.toString());
        logger.info("acknowledgment===>"+acknowledgment);
        acknowledgment.acknowledge();
        latch.countDown();
    }



 @Override
    public void onMessage(Object data, Acknowledgment acknowledgment) {

    }

    @Override
    public void onMessage(Object data, Consumer consumer) {

    }

    @Override
    public void onMessage(Object data, Acknowledgment acknowledgment, Consumer consumer) {

    }

    @Override
    public void onMessage(Object data) {

    }
}
