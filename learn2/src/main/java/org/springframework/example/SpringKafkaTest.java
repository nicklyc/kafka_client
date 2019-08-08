package org.springframework.example;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.example.learn2.consumer.ConsumerConf;
import org.apache.example.learn2.producer.ProducerConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

/**
 * 原生使用
 * 
 * @author maochao
 * @since 2019/8/6 15:50
 */
public class SpringKafkaTest {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaTest.class);

    @Test
    public void testAutoCommit() throws Exception {
        logger.info("Start auto");
        ContainerProperties containerProps = new ContainerProperties("learn2", "topic2");
        final CountDownLatch latch = new CountDownLatch(4);
        containerProps.setMessageListener(new MyMessageListener(latch));

        KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
        container.setBeanName("testAuto");
        container.start();

        latch.await(60, TimeUnit.SECONDS);
        container.stop();
        logger.info("Stop auto");
    }

    /**
     * 发送消息
     */
    @Test
    public void sendMess() {
        KafkaTemplate<Integer, String> template = createTemplate();
        template.setDefaultTopic("learn2");
        // 发送一条记录
        template.sendDefault(0, "foo");
        template.sendDefault(2, "bar");
        template.sendDefault(0, "baz");
        template.sendDefault(2, "qux");
        template.flush();
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        // 消费者工厂
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
        // 单线程container实现，只启动一个consume
        KafkaMessageListenerContainer<Integer, String> container =
            new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    /**
     * 创建模板
     * 
     * @return
     */
    private KafkaTemplate<Integer, String> createTemplate() {
        // 构建生产者factory
        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory(producerProps());
        return new KafkaTemplate<>(pf);
    }

    /**
     * 消费者配置
     * 
     * @return
     */
    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConf.url);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId123");
        // 自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    /**
     * 生产者配置
     * 
     * @return
     */
    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConf.url);
        // 不进行重试
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 延迟1毫秒
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
