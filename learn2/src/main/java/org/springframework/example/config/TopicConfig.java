package org.springframework.example.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.example.learn2.producer.ProducerConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Topic 配置
 * @author maochao
 * @since 2019/8/6 20:11
 */
@Configuration
public class TopicConfig {

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConf.url);
        return new KafkaAdmin(configs);
    }

    @Bean(name="learn")
    public NewTopic topicLearn() {
        return new NewTopic("foo", 10, (short) 1);
    }

    @Bean(name="bar")
    public NewTopic TopciBar() {
        return new NewTopic("bar", 10, (short) 1);
    }
}
