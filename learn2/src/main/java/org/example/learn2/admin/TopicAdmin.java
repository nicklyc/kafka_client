package org.example.learn2.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 操作topic的类
 * 
 * @author maochao
 * @since 2019/7/31 21:02
 */
public class TopicAdmin {
    public static final String url = "39.100.104.199:9092";
    public static AdminClient client;

    public static void main(String[] args) {
        createTopics();
    }


    /***
     * 创建 topics
     */
    static void createTopics() {
        NewTopic topic = new NewTopic("admin_create", 4, (short)1);
        CreateTopicsResult result = client.createTopics(Arrays.asList(topic));
        try {
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化 client
     */
    static {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        // 创建topic
        client = KafkaAdminClient.create(properties);
    }

}
