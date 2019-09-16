package org.apache.example.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 操作topic的类
 * 
 * @author maochao
 * @since 2019/7/31 21:02
 */
public class TopicAdmin {

    public static final String url = "39.100.104.199:9093,39.100.104.199:9094";

    public static AdminClient client;

    public static void main(String[] args) {
        createTopics();
    }



    /***
     * 创建 topics
     */
    @Test
    public void  create() {
        NewTopic topic = new NewTopic("admin_c111", 1, (short)2);
        CreateTopicsResult topics = client().createTopics(Arrays.asList(topic));

        KafkaFuture<Void> all = topics.all();

        try {
            all.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void  detailTopic() {

        DescribeTopicsResult admin_create5 = client().describeTopics(Arrays.asList("admin_create10"));

        Map<String, KafkaFuture<TopicDescription>> values = admin_create5.values();

        KafkaFuture<TopicDescription> topic = values.get("admin_create10");
        TopicDescription topicDescription = null;
        try {
            topicDescription = topic.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<TopicPartitionInfo> partitions = topicDescription.partitions();

        partitions.forEach(partition->{
            System.out.println(">>>> "+partition.toString());
        });
    }




    /**
     * 初始化 client
     * @return
     */
    public AdminClient client() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }
    /**
     *初始化 KafkaAdmin
     * @return
     */
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        KafkaAdmin admin = new KafkaAdmin(props);
        return admin;
    }

    /***
     * 创建 topics
     */
    static void createTopics() {
        NewTopic topic = new NewTopic("admin_create", 4, (short)2);
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
