package org.example.learn2.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者配置
 *
 * @author maochao
 * @date 2019/6/15
 */
public class ProducerConf {
    public static final String url = "39.100.104.199:9092";
    public static final String topic = "learn2";

    public static Properties initProperties() {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 请求最大的超时时间
        properties.put("request.timeout.ms", 50);
        // 元数据更新周期
         properties.put("metadata.max.age.ms", 5000);
        // 自定义拦截器
        // properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Interceptor.class.getName());
        // Kryo 序列化
       // properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class.getName());
        //protostuff序列化
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1");

       // properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000*8);
         //properties.put(ProducerConfig.BATCH_SIZE_CONFIG,50);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"lz4");

        return properties;
    }
}
