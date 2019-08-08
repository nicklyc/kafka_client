package org.example.learn2.spring;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CountDownLatch;

/**
 * 自动提交 ，或者容器管理提交
 * 
 * @author maochao
 * @since 2019/8/6 16:41
 */
public class MyMessageListener implements MessageListener<Integer, String> {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaTest.class);

    private CountDownLatch latch;

    public MyMessageListener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onMessage(ConsumerRecord<Integer, String> data) {
        logger.info("收到消息===>" + data.toString());
        latch.countDown();
    }

}
