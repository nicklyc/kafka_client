package org.springframework.example.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Resource;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.example.learn2.producer.ProducerConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author maochao
 * @since 2019/8/6 18:50
 */
@Configuration
public class SpringKafkaTemplate {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaTemplate.class);
    @Resource
    KafkaTemplate kafkaTemplate;

    @Test
    public void senTest() {
        kafkaTemplate = createTemplate();
        kafkaTemplate.setProducerListener(new MyproducerListener());
        ListenableFuture future = kafkaTemplate.send("learn2", 2, "异步发送");
        future.addCallback(new ProducerCallback());


        //同步发送：
        try {
            final ProducerRecord<String, String> record =new ProducerRecord("learn2","同步发送");
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
            logger.info("发送成功" +record.toString());
            //handleSuccess(data);
        }
        catch (ExecutionException e) {
            logger.info("发送失败",e);
           // handleFailure(data, record, e.getCause());
        }
        catch (TimeoutException | InterruptedException e) {
            //handleFailure(data, record, e);
        }
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

    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConf.url);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
