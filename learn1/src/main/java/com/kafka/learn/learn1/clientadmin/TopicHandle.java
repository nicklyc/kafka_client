package com.kafka.learn.learn1.clientadmin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author
 * @date 2019/5/18
 * 快速开始 消费者
 *
 */
public class TopicHandle {
    public static final String url = "192.168.208.128:9092";
    public static void main(String[] args) {

       // addTopics();
       //getTopics();






    }


    /**
     * 创建topics
     *
     */
    @Test
    public  void addTopics(){
         NewTopic topic_b = new NewTopic("topic_b", 4, (short) 1);
         NewTopic topic_a = new NewTopic("topic_a", 4, (short) 2);
         List<NewTopic> topicList = new ArrayList<>(2);
        // topicList.add(topic_b);
         topicList.add(topic_a);
        AdminClient client = getAdminClient();
        client.createTopics(topicList);
        client.close();
     }

    /**
     * 创建topics
     *
     */
    @Test
    public  void getTopics(){

        AdminClient client = getAdminClient();

        ListTopicsResult topics = client.listTopics();

        KafkaFuture<Set<String>> names = topics.names();

        try {
            Set<String> namesSet = names.get();
            System.out.println(namesSet);
        } catch (Exception e) {
            e.printStackTrace();
        }

        KafkaFuture<Collection<TopicListing>> listings = topics.listings();

        KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = topics.namesToListings();

      // client.close();
    }



    @Test
    public  void getTopicsByName(){

        AdminClient client = getAdminClient();
        List<String> topicList = new ArrayList<>(2);
        topicList.add("topic_b");
        client.describeTopics(topicList);
        client.close();
    }

    /**
     * 初始化客户端
     * @return
     */
    public  AdminClient getAdminClient(){
        Properties props = new Properties();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        //TODO  可以添加很多配置属性
        //...其他配置
        return AdminClient.create(props);
    }


    /**创建topic 流程分析：
     * AdminClient提供了静态方法create
     * AdminClient.create(props)
     *
     * ---> KafkaAdminClient.createInternal(new AdminClientConfig(props)
     *   初始化一个AdminClientConfig，关于创建topic的很多配置就在这个类中，观察AdminClientConfig的就知道，
     *       ---->AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
     *                 config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
     *                 config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
     *        ---> new KafkaAdminClient(config, clientId, time, metadataManager, metrics, networkClient,
     *                 timeoutProcessorFactory, logContext);
     *
     *config ：在这里，实现用户配置替换默认配置，
     *clientId ：从client.id中读取，没有就自动生成 "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
     *metadataManager：  mananger的元数据  由retry.backoff.ms和 metadata.max.age.ms 配置初始化，
     *     retry.backoff.ms：失败进行重试的间隔时间，
     *     metadata.max.age.ms： 强制更新metedata的时间
     *networkClient：
     *       networkClient = new NetworkClient(
     *                 selector,
     *                 metadataManager.updater(),
     *                 clientId,
     *                 1,
     *                 config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
     *                 config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
     *                 config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
     *                 config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
     *                 (int) TimeUnit.HOURS.toMillis(1),
     *                 time,
     *                 true,
     *                 apiVersions,
     *                 logContext);
     *  reconnect.backoff.ms： 重连时间 默认50 毫秒
     *  reconnect.backoff.max.ms：重连最大时间，
     *  send.buffer.bytes ：发送数据的TCP缓冲区大小  默认值65536
     *  receive.buffer.bytes：读取数据的缓冲区大小   默认值65536   使用-1 将使用操作系统的缓冲区
     *
     *  selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
     *                     metrics, time, metricGrpPrefix, channelBuilder, logContext);
     *  connections.max.idle.ms ：空闲连接的回收时间 默认540000
     *
     *
     *createTopics（）；
     *---//创建一组topics 非原子性，可以部分成功
     *createTopics(Collection<NewTopic> newTopics,
     *             CreateTopicsOptions options);
     *在newTopics 中封装了 topic 名字，分区数量，负载因子。
     *   ---CreateTopicsRequest
     */


    /**
     *
     * //对
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */


}
