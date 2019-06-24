/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.serialization.ExtendedSerializer.Wrapper.ensureExtended;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster
 * than having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the
 * key/value pairs.
 * 
 * <pre>
 * {
 *     &#64;code
 *     Properties props = new Properties();
 *     props.put("bootstrap.servers", "localhost:9092");
 *     props.put("acks", "all");
 *     props.put("retries", 0);
 *     props.put("batch.size", 16384);
 *     props.put("linger.ms", 1);
 *     props.put("buffer.memory", 33554432);
 *     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 *     Producer<String, String> producer = new KafkaProducer<>(props);
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 *     producer.close();
 * }
 * </pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server as
 * well as a background I/O thread that is responsible for turning these records into requests and transmitting them to
 * the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of
 * pending record sends and immediately returns. This allows the producer to batch together individual records for
 * efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting we
 * have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code> as 0
 * it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by the
 * <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we
 * will generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However
 * if you want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This
 * will instruct the producer to wait up to that number of milliseconds before sending a request in hope that more
 * records will arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the
 * code snippet above, likely all 100 records would be sent in a single request since we set our linger time to 1
 * millisecond. However this setting would add 1 millisecond of latency to our request waiting for more records to
 * arrive if we didn't fill up the buffer. Note that records that arrive close together in time will generally batch
 * together even with <code>linger.ms=0</code> so under heavy load batching will occur regardless of the linger
 * configuration; however setting this to something larger than 0 can lead to fewer, more efficient requests when not
 * under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If
 * records are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the
 * buffer space is exhausted additional send calls will block. The threshold for time to block is determined by
 * <code>max.block.ms</code> after which it throws a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the
 * user provides with their <code>ProducerRecord</code> into bytes. You can use the included
 * {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional
 * producer. The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery.
 * In particular producer retries will no longer introduce duplicates. The transactional producer allows an application
 * to send messages to multiple partitions (and topics!) atomically.
 * </p>
 * <p>
 * To enable idempotence, the <code>enable.idempotence</code> configuration must be set to true. If set, the
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will not
 * need to be modified to take advantage of this feature.
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a
 * {@link #send(ProducerRecord)} returns an error even with infinite retries (for instance if the message expires in the
 * buffer before being sent), then it is recommended to shut down the producer and check the contents of the last
 * produced message to ensure that it is not duplicated. Finally, the producer can only guarantee idempotence for
 * messages sent within a single session.
 * </p>
 * <p>
 * To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be
 * configured for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and
 * the <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional
 * guarantees to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful,
 * application. As such, it should be unique to each producer instance running within a partitioned application.
 * </p>
 * <p>
 * All the new transactional APIs are blocking and will throw exceptions on failure. The example below illustrates how
 * the new APIs are meant to be used. It is similar to the example above, except that all 100 messages are part of a
 * single transaction.
 * </p>
 * <p>
 * 
 * <pre>
 * {
 *     &#64;code
 *     Properties props = new Properties();
 *     props.put("bootstrap.servers", "localhost:9092");
 *     props.put("transactional.id", "my-transactional-id");
 *     Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 *     producer.initTransactions();
 *
 *     try {
 *         producer.beginTransaction();
 *         for (int i = 0; i < 100; i++)
 *             producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *         producer.commitTransaction();
 *     } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *         // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *         producer.close();
 *     } catch (KafkaException e) {
 *         // For all other exceptions, just abort the transaction and try again.
 *         producer.abortTransaction();
 *     }
 *     producer.close();
 * }
 * </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required to specify
 * callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the <code>producer.send()</code> or transactional calls hit an
 * irrecoverable error during a transaction. See the {@link #send(ProducerRecord)} documentation for more details about
 * detecting errors from a transactional send.
 * </p>
 * </p>
 * By calling <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that
 * any successful writes are marked as aborted, hence keeping the transactional guarantees.
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features. For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive
 * an <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final ExtendedSerializer<K> keySerializer;
    private final ExtendedSerializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    private TransactionalRequestResult initTransactionsResult;

    /**
     * [1] 构造器重载方法
     *
     * @param configs
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null, null, null);
    }

    /**
     * [2] 构造器重载方法2
     *
     * @param configs 配置
     * @param keySerializer key 序列化器
     * @param valueSerializer value 序列化器
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
            keySerializer, valueSerializer, null, null);
    }

    /**
     * [3] KafkaProducer 构造器重载3 同构造方法1 具体配置可以查看 <a href="http://kafka.apache.org/documentation.html#producerconfigs">
     *
     * @param properties The producer configs 键值对配置，
     */
    public KafkaProducer(Properties properties) {
        // 将properties-->得到一个ProducerConfig对象
        this(new ProducerConfig(properties), null, null, null, null);
    }

    /**
     * [4] 构造器重载方法4 同构造方法2
     *
     * @param properties
     * @param keySerializer
     * @param valueSerializer
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
            keySerializer, valueSerializer, null, null);
    }

    /**
     * 【5】 KafkaProducer 核心构造方法。 前4个构造器都是调用的该构造器对KafkaProducer 进行实例化 也就是通过构造方法获取KafkaProducer的统一入口。
     * <p>
     * 由于该方法没有使用public 修饰，仅仅内部实例化调用， 我们一般使用[3] 进行实例化，就是一切可配置
     * <p>
     * 具体config的配置可以查看 <a href="http://kafka.apache.org/documentation.html#producerconfigs"> 或者查看ProducerConfig类就行。
     * <p>
     * 在使用完KafkaProducer。一定要注意调用close,方法，防止内存泄漏
     * <li>Note: you must always {@link #close()} it to avoid resource leaks.</li>
     * 
     *
     * @param config
     * @param keySerializer
     * @param valueSerializer
     * @param metadata
     * @param kafkaClient
     */
    KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer, Metadata metadata,
        KafkaClient kafkaClient) {
        try {
            /**
             * 读取用户自定义配置
             *
             * 注意： 到这里的时候，用户自定义配置和默认初始化配置已经 在ProducerConfig中初始化完成了， 具体查看在调用核心构造方法前的其他构造方法中初始化配置的的源码
             */

            Map<String, Object> userProvidedConfigs = config.originals();
            // 获取所有的配置，包括默认的初始化配置
            this.producerConfig = config;
            this.time = Time.SYSTEM;

            /**
             * 获取配置client.id配置，这个配置可以帮助追踪消息来源 这个clientId 发送消息的时候，会被服务器记录，可用于追踪消息的原来 没有对clientId进行配置 将会自动生成 producer-1，
             * producer-2，...格式的clientId
             */

            String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;
            /** kafka对事务的处理，暂时都没学习，关于事务，后面统一分析，补上 */
            String transactionalId = userProvidedConfigs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
                ? (String)userProvidedConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) : null;
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(
                    String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig =
                new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(
                        Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters =
                config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
            /**
             * 读取使用的分区器 获取Partitioner的一个实例
             *
             * 分区器配置名properties.put("partitioner.class" ,"全类名") 如果配置了将会使用用户自定义的分区器，如果用户没有自定义分区器，这里将会使用 class
             * org.apache.kafka.clients.producer.internals.DefaultPartitioner 分区器进行分区。 具体kafka是如何实现这个功能的点进去源码分析
             * config.getConfiguredInstance(),就是读取配置中的一个，接口实例
             */

            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            // 读取retry.backoff.ms 配置，失败重试的间隔时间
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            if (keySerializer == null) {
                /**
                 * 读取序列化器
                 *
                 * config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,Serializer.class))
                 * 就是通过配置值获取一个 Serializer的接口实例， ensureExtended就是对这个实例进行增强，
                 */
                this.keySerializer = ensureExtended(
                    config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class));
                // 对序列化器进行配置
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = ensureExtended(keySerializer);
            }
            if (valueSerializer == null) {
                // 读取value序列化器
                this.valueSerializer = ensureExtended(
                    config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class));
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = ensureExtended(valueSerializer);
            }

            /** 用户自定义配置中加入client.id 配置 获取配置的拦截器，可能配置多个，采用list */
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            List<ProducerInterceptor<K, V>> interceptorList = (List)(new ProducerConfig(userProvidedConfigs, false))
                .getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);

            this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners =
                configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, reporters);
            // 读取max.request.size配置值
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            // 读取buffer.memory配置值
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            // 读取压缩类型compression.type 配置
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            // 读取最大阻塞时间配置 max.block.ms
            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            /**
             * <p>
             * 读取request.timeout.ms 请求超时时间 等待响应的超时时间
             * </p>
             * 超时后会进行重试，这个值的设置应该大于 replica.lag.time.max.ms 减少由于不必要的生产者重试
             */
            this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.transactionManager = configureTransactionState(config, logContext, log);
            /**
             * <p>
             * 重试次数
             * <li>读取retries 配置。当有事务的时候，如果用户没有配置这个值，会一直重试</li>
             * <li>如果允许重试那么考虑消息的顺序问题</li>
             * <li>可以结合max.in.flight.requests.per.connection的配置值进行顺序保证,但该配置会引起吞吐量问题/li>
             * </p>
             */
            int retries = configureRetries(config, transactionManager != null, log);
            /**
             * <ul>
             * <li>读取max.in.flight.requests.per.connection 配置</li>
             * <li>限制客户端在单个连接上能够发送的未响应请求的个数</li>
             * <li>默认值是5，也就是5个 请求未返回，producer将会被阻塞，</li>
             * <li>使用了事务，该值不允许大于 5</li>
             * <li>max.in.flight.requests.per.connection设置1 就表明 :只能等上个消息得到response 之后，才能发生下一个请求
             * 这样可以保证消息的顺序性，但是吞吐量会下降很多</li>
             * </ul>
             */
            int maxInflightRequests = configureInflightRequests(config, transactionManager != null);
            /**
             * <ul>
             * <li>获取ack配置： producer收到的ack的配置。默认值是1，有四种配置 in("all", "-1", "0", "1")</li>
             * </ul>
             * <ul acks设置为：0 >
             * <li>不需要服务器的ack,producer 将消息发送到缓冲区就会认为完成了消息发送</li>
             * <li>也就是producer感知不到发送失败，会认为所有发送的消息都是被服务器接受</li>
             * <li>配置的retries 也不会有什么意义，</li>
             * <li>每个记录返回的偏移量将始终设置为-1</li>
             * </ul>
             * <ul acks设置为：1>
             * <li>在集群环境下，当服务端的leader节点收到了请求，写入日志文件，就会立即响应， 也就是收到leader ack 认为成功，</li>
             * <li>这种情况，除非leader ack后挂了，其他节点数据也没有被同步过去这种情况存在数据丢失</li>
             * </ul>
             ** <ul acks设置为：all，-1>
             * <li>all的配置会在这里转化为 -1</li>
             * <li>就是收到所有的副本节点数据同步成功，ack，不会存在数据丢失，可靠性投递的最高保障 但是性能也是最低的</li> *
             * </ul>
             */
            short acks = configureAcks(config, transactionManager != null, log);

            this.apiVersions = new ApiVersions();

            // 初始化accumulator
            this.accumulator = new RecordAccumulator(logContext, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                this.totalMemorySize, this.compressionType, config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                retryBackoffMs, metrics, time, apiVersions, transactionManager);
            /** 获取服务器地址列表 */
            List<InetSocketAddress> addresses =
                ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                // 初始化创建metadata实例
                this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                    true, true, clusterResourceListeners);
                this.metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());
            }
            /**
             * 创建通道构建器， ChannelBuilder是一认证接口,分别有具体的实现类
             * 
             * 
             * SaslChannelBuilder <br/>
             * SslChannelBuilder <br/>
             * PlaintextChannelBuilder <br/>
             * 
             * 
             * 这里 会根据配置不同的安全协议，创建一个相应类型的ChannelBuilder实例
             */
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config);
           //监控数据
            Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
           
            /**
             * 获取kafka的client NetworkClient
             * 
             * NetworkClient是kafka客户端的网络接口层，实现了接口KafkaClient，封装了Java NIO对网络的调用 <br>
             * 
             * producer 与broker 建立的网络连接这块底层知识 比较难， 暂不深入分析，后期会细节突破
             */
               
            // 获取一个selector connections.max.idle.ms
            Selector selector = new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                this.metrics, time, "producer", channelBuilder, logContext);

            /**
             * 实例化KafkaClient ,一下都是KafkaClient的配置
             *
             * reconnect.backoff.ms
             *
             * reconnect.backoff.max.ms
             *
             * send.buffer.bytes
             *
             * receive.buffer.bytes
             *
             * request.timeout.ms
             *
             * max.in.flight.requests.per.connection
             *
             */
            KafkaClient client = kafkaClient != null ? kafkaClient : 
                    new NetworkClient(
                            selector,
                            this.metadata, clientId, maxInflightRequests,
                            config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                            config.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                            config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                            config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                            this.requestTimeoutMs,
                            time, 
                            true,
                            apiVersions, 
                            throttleTimeSensor, 
                            logContext
                    );



            /**
             * 初始化sender线程
             * 
             * 这个线程中将从broker获取metadata 信息 <br>
             * 从accumulator中获取消息并发送给broker
             *
             * max.request.size
             * request.timeout.ms
             * retry.backoff.ms
             */
            this.sender = new Sender(logContext, client, this.metadata, this.accumulator, maxInflightRequests == 1,
                config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), acks, retries, metricsRegistry.senderMetrics,
                Time.SYSTEM, this.requestTimeoutMs, config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager, apiVersions);

            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;

            /**
             * 开启sender线程，并设置为主线程的守护线程
             * 这里设置成守护线程有很多好处。我们只需要维护producer线程就可以了，不需要另外维护
             * sender线程，在一些后台功能的线程中守护线程的应用场景很多：
             * 比如：JVM 的GC线程，比如心跳管理线程。
             */
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            this.errors = this.metrics.sensor("errors");
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see
            // KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext,
        Logger log) {

        TransactionManager transactionManager = null;

        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
            userConfiguredIdempotence = true;

        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
            userConfiguredTransactions = true;

        boolean idempotenceEnabled = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException(
                "Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");

        if (userConfiguredTransactions)
            idempotenceEnabled = true;

        if (idempotenceEnabled) {
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager =
                new TransactionManager(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs);
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }

        return transactionManager;
    }

    /**
     * 获取重试次数
     * 
     * @param config 配置
     * @param idempotenceEnabled
     * @param log
     * @return
     */
    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        // 判断用户是否配置重试次数
        if (config.originals().containsKey(ProducerConfig.RETRIES_CONFIG)) {
            userConfiguredRetries = true;
        }
        // 用户没有配置，事务开启的情况
        if (idempotenceEnabled && !userConfiguredRetries) {
            log.info("Overriding the default retries config to the recommended value of {} since the idempotent "
                + "producer is enabled.", Integer.MAX_VALUE);
            // 无限重试
            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException(
                "Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        // 使用配置值
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
    }

    /**
     * 获取一个connection 上的最大 max.in.flight.requests.per.connection数量
     * 
     * @param config
     * @param idempotenceEnabled
     * @return
     */
    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        // 存在事务的情况下，只能设置小于5
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
                + " to at most 5" + " to use the idempotent producer.");
        }
        // 获取配置值
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    /**
     * 获取acks 配置，默认值为1， 如果开启了事务，只允许配置为-1 或者all,保证数据的可靠性
     * 
     * @param config
     * @param idempotenceEnabled
     * @param log
     * @return
     */
    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        // 获取acks
        short acks = (short)parseAcks(config.getString(ProducerConfig.ACKS_CONFIG));
        // 用户是否配置ack
        if (config.originals().containsKey(ProducerConfig.ACKS_CONFIG)) {
            userConfiguredAcks = true;
        }

        if (idempotenceEnabled && !userConfiguredAcks) {
            log.info("Overriding the default {} to all since idempotence is enabled.", ProducerConfig.ACKS_CONFIG);
            return -1;
        }
        // 事务条件下，acks需要设置为 -1
        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG
                + " to all in order to use the idempotent " + "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    /**
     * acks的配置，没有配置使用默认值，否则使用用户配置，将all 转为 -1
     * 
     * @param acksString
     * @return
     */
    private static int parseAcks(String acksString) {
        try {

            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     * <p>
     * This method does the following: 1. Ensures any transactions initiated by previous instances of the producer with
     * the same transactional.id are completed. If the previous instance had failed with a transaction in progress, it
     * will be aborted. If the last transaction had begun completion, but not yet finished, this method awaits its
     * completion. 2. Gets the internal producer id and epoch, used in all future transactional messages issued by the
     * producer.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot be initialized before
     * expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted. It is
     * safe to retry in either case, but once the transactional state has been successfully initialized, this method
     * should no longer be used.
     *
     * @throws IllegalStateException if no transactional.id has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker does not
     *             support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws AuthorizationException fatal error indicating that the configured transactional.id is not authorized. See
     *             the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed
     *             <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Override
    public void initTransactions() {
        throwIfNoTransactionManager();
        if (initTransactionsResult == null) {
            initTransactionsResult = transactionManager.initializeTransactions();
            sender.wakeup();
        }

        try {
            if (initTransactionsResult.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)) {
                initTransactionsResult = null;
            } else {
                throw new TimeoutException(
                    "Timeout expired while initializing transactional state in " + maxBlockTimeMs + "ms.");
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Initialize transactions interrupted.", e);
        }
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation of this
     * method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()} has
     *             not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker does not
     *             support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws AuthorizationException fatal error indicating that the configured transactional.id is not authorized. See
     *             the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    @Override
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks those offsets as part of the
     * current transaction. These offsets will be considered committed only if the transaction is committed
     * successfully. The committed offset should be the next message your application will consume, i.e.
     * lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages together, typically in a
     * consume-transform-produce pattern. Thus, the specified {@code consumerGroupId} should be the same as config
     * parameter {@code group.id} of the used {@link KafkaConsumer consumer}. Note, that the consumer should have
     * {@code enable.auto.commit=false} and should also not commit offsets manually (via
     * {@link KafkaConsumer#commitSync(Map) sync} or {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async}
     * commits).
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker does not
     *             support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *             format used for the offsets topic on the broker does not support transactions
     * @throws AuthorizationException fatal error indicating that the configured transactional.id is not authorized. See
     *             the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any other
     *             unexpected error
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
        throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        sender.wakeup();
        result.await();
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the
     * transaction.
     * <p>
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker does not
     *             support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws AuthorizationException fatal error indicating that the configured transactional.id is not authorized. See
     *             the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any other
     *             unexpected error
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await();
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made. This call
     * will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link AuthorizationException}.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker does not
     *             support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws AuthorizationException fatal error indicating that the configured transactional.id is not authorized. See
     *             the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    @Override
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await();
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>. See
     * {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * 异步发送消息
     * 收到ack时执行回调方法
     *
     * 这个方法的本身是异步的，因为返回的结果是一个future, 可以通过future实现
     * 发送消息的串行化，进而实现同步发送，但是这个同步跟这个方法没有关系。
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset it
     * was assigned and the timestamp of the record. If {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME
     * CreateTime} is used by the topic, the timestamp will be the user provided timestamp or the record send time if
     * the user did not specify a timestamp for the record. If
     * {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the topic, the
     * timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link Future Future} for the {@link RecordMetadata} that will
     * be assigned to this record. Invoking {@link Future#get() get()} on this future will block until the associated
     * request completes and then return the metadata for the record or throw any exception that occurred while sending
     * the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }
     * </pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that will be
     * invoked when the request is complete.
     *
     * <pre>
     * {
     *     &#64;code
     *     ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("the-topic", key, value);
     *     producer.send(myRecord, new Callback() {
     *         public void onCompletion(RecordMetadata metadata, Exception e) {
     *             if (e != null) {
     *                 e.printStackTrace();
     *             } else {
     *                 System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *             }
     *         }
     *     });
     * }
     * </pre>
     * <p>
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future in
     * order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error, the
     * final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When this
     * happens, your application should call {@link #abortTransaction()} to reset the state and continue to send data.
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}. In particular, if a
     * transactional send finishes with a {@link ProducerFencedException}, a
     * {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, a
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an {@link AuthorizationException}, then
     * the only option left is to call {@link #close()}. Fatal errors cause the producer to enter a defunct state in
     * which future API calls will continue to raise the same underyling error wrapped in a new {@link KafkaException}.
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured. In
     * this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and {@link AuthorizationException}
     * are considered fatal errors. However, {@link ProducerFencedException} does not need to be handled. Additionally,
     * it is possible to continue sending after receiving an
     * {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so can result in out of order
     * delivery of pending messages. To ensure proper ordering, you should close the producer and create a new instance.
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional produce
     * requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException} error. If
     * this is encountered during a transaction, it is possible to abort and continue. But note that future sends to the
     * same topic will continue receiving the same exception until the topic is upgraded.
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *            indicates no callback)
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *             when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the time taken for fetching metadata or allocating memory for the record has
     *             surpassed <code>max.block.ms</code>.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        //拦截器处理
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    private void throwIfProducerClosed() {
        if (ioThread == null || !ioThread.isAlive())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * 将消息追加到RecordAccumulator缓存队列，并唤醒sender线程
     * @param record
     * @param callback
     * @return
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            throwIfProducerClosed();
            // first make sure the metadata for the topic is available
            //在数据发送前，需要先该 topic 是可用的
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                /**等待metadata的更新
                 *
                 * 这里就是更新的是Kafka的Cluster的信息，还有包括一些对应关系，关联联系，和一些逻辑数据
                 */
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName()
                    + " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName()
                    + " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName()
                    + " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName()
                    + " specified in value.serializer", cce);
            }
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                compressionType, serializedKey, serializedValue, headers);
            ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(),
                partition);
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);

            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                serializedValue, headers, interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch",
                    record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders)headers).setReadOnly();
        }
    }

    /**
     * 等待集群的元数据，
     * Wait for cluster metadata including partitions for the given topic to be available.
     *
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after
     *             producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs)
        throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        /**
         * 将topic的名字加入metadata进行维护
         * 
         * 在 metadata 中添加 topic 后,并重置过期时间，
         * 
         * 如果 metadata 中没有这个 topic 的 meta，也就是新加入的topic，打开更新开关needUpdate=true
         * 
         */
        metadata.add(topic);
        /**
         * metadata 在KafkaProducer 核心构造方法中实例化了
         * 获取Cluster 信息
         */
        Cluster cluster = metadata.fetch();
        /**
         * 获取topic 的分区数
         */
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        /**
         * 如果medata中可已经维护的有该topic 的元数据，并且需要更新的分区号在 metadata维护的数据中，
         * 那么就直接返回该medata中的cluster 信息，
         */
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        long elapsed;
        // Issue metadata requests until we have metadata for the topic or maxWaitTimeMs is exceeded.
        // In case we already have cached metadata for the topic, but the requested partition is greater
        // than expected, issue an update request only once. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.

        /**
         * 当是一个新的topic 或者新分区进行metadata 更新
         * 
         * 使用了一个do while()循环进行更新数据， metadata.awaitUpdate(version, remainingWaitMs); while (partitionsCount ==
         * null)一直更新到partitionsCount！=null;
         * 更新数据结束，需要看是否超过了max.block.ms配置的最大阻塞时间
         * 并且还要检查指定分区号是否可用
         *
         *  主线程 会阻塞在两个 while 循环中，直到 metadata 信息更新，
         *
         *  真正负责更新的是守护线程sender，主线程使用了两层whlie循环实现轮询更新的结果。
         */

        do {
            log.trace("Requesting metadata update for topic {}.", topic);
            metadata.add(topic);
            int version = metadata.requestUpdate();
            //唤醒sender线程
            sender.wakeup();
            try {
                //等待更新cluster信息的阻塞方法
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            //更新数据时间超过 max.block.ms
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            if (cluster.unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null);

        //再次校验partition是否合法。主要是校验partition是否超过了可用分区
        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(String.format(
                "Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /** Validate that the record size isn't too large */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size
                + " bytes when serialized which is larger than the maximum request size you have configured with the "
                + ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size
                + " bytes when serialized which is larger than the total memory buffer you have configured with the "
                + ProducerConfig.BUFFER_MEMORY_CONFIG + " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition of
     * <code>flush()</code> is that any previously sent record will have completed (e.g.
     * <code>Future.isDone() == true</code>). A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The
     * <code>flush()</code> call gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * 
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     * <p>
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does
     * not occur we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()}
     * will flush all buffered records before performing the commit. This ensures that all the
     * {@link #send(ProducerRecord)} calls made since the previous {@link #beginTransaction()} are completed before the
     * commit.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after
     *             producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /** Get the full set of internal metrics maintained by the producer. */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete. This method is equivalent to
     * <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0,
     * TimeUnit.MILLISECONDS) will be called instead. We do this because the sender thread would otherwise try to join
     * itself and block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail any unsent
     * and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while blocking
     * the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *            non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn(
                    "Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. "
                        + "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                    timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed "
                + "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        ClientUtils.closeQuietly(partitioner, "producer partitioner", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka producer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException)exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer,
        Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    /**
     * computes partition for given record. if the record has partition returns the value otherwise calls configured
     * partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ? partition : partitioner.partition(record.topic(), record.key(), serializedKey,
            record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions "
                + "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    /**
     * 使用了一个静态内部类，封装了等待元数据更新的返回值， waitedOnMetadataMs 和 cluster
     */
    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;

        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata
                : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
