package com.kafka.learn2.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者配置
 *
 * @author maochao
 * @date 2019/6/15
 */
public class ProducerProperties {
    public static final String url = "192.168.208.128:9092";
    public static final String topic = "quick_start_m";

    public static Properties initProperties(){

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        return properties;
    };
}
