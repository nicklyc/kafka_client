/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 元数据
 * 维护的就是一些topic相关的数据，最主要的是Cluster 对象，其他的属于逻辑变量。
 * 所有public方法都是synchronized修饰了，线程安全类
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * <p>
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;
    /**
     * 刷新重试时间，KafkaProduer 实例化Metadata时候 采用的是 retry.backoff.ms配置
     * 在这里指的时候  metadata的更新周期，
     */
    private final long refreshBackoffMs;
    /**
     * metadata 的过期时间, 默认 60,000ms
     */
    private final long metadataExpireMs;
    /**
     * 版本号
     */
    private int version;
    /**
     * 上一次更新时间
     */
    private long lastRefreshMs;
    /**
     * 上一次成功更新时间
     */
    private long lastSuccessfulRefreshMs;
    private AuthenticationException authenticationException;
    /**
     * 集群数据，核心数据
     * 其他的是逻辑数据，帮助业务处理
     * cluster是关键数据
     */
    private Cluster cluster;
    /**
     * 更新开关
     */
    private boolean needUpdate;
    /**
     * topic的名字和过期时间映射关系
     */
    private final Map<String, Long> topics;
    private final List<Listener> listeners;
    private final ClusterResourceListeners clusterResourceListeners;
    /**
     * 更新所有的topic开关
     */
    private boolean needMetadataForAllTopics;
    private final boolean allowAutoTopicCreation;
    /**
     * 过期处理
     */
    private final boolean topicExpiryEnabled;
    private boolean isClosed;

    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation) {
        this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param allowAutoTopicCreation   If this and the broker config 'auto.create.topics.enable' are true, topics that
     *                                 don't exist will be created by the broker when a metadata request is sent
     * @param topicExpiryEnabled       If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
                    boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
        this.isClosed = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * 添加新的topic 的名字到Metadata中进行维护，
     * 会重置过期时间
     * 刷新上一次更新时间，并且设置更新为true；
     */
    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     * 计算下次更新metadata时间，这里也就是metadata更新策略：
     *
     *   1: 当needUpdate=true时，当前时间 - 上一次更新时间 > refreshBackoffMs 进行更新。
     *      否则，继续等待到更新周期refreshBackoffMs进行跟新，
     *
     *   2:如果needUpdate=false时候，检查metadata是否过期，如果过期了。则对更新周期进行判断。
     *
     *   3: refreshBackoffMs使用的是 retry.backoff.ms配置。默认更新周期100毫秒。
     *
     *   总结：metadata的更新周期是100ms，过期时间是metadata.max.age.ms，默认60s。
     *   如果当needUpdate=true，当更新开关打开，并且与上一次更新时间间隔达到100ms以上进行更新，
     *   在更新开关没有打开的情况下，上一次更新是一个成功的操作，该metadata会缓存metadata.max.age.ms,
     *   然后数据过期发生更新操作。
     *
     *   timeToNextUpdate（nowMs）的返回值就是还剩多久需要进行更新操作。
     *
     */

    public synchronized long timeToNextUpdate(long nowMs) {

        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);

        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * If any non-retriable authentication exceptions were encountered during
     * metadata update, clear and return the exception.
     */
    public synchronized AuthenticationException getAndClearAuthenticationException() {
        if (authenticationException != null) {
            AuthenticationException exception = authenticationException;
            authenticationException = null;
            return exception;
        } else
            return null;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 更新metadata,只要版本号没有更新并且metadata没有close,就会一直调用wait阻塞，
     * 当阻塞总时间超过max.block.ms，就会抛出超时异常。
     *
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0)
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");

        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        while ((this.version <= lastVersion) && !isClosed()) {
            AuthenticationException ex = getAndClearAuthenticationException();
            if (ex != null)
                throw ex;
            if (remainingWaitMs != 0)
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     *
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics)) {
            requestUpdateForNewTopics();
        }
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     *
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param newCluster        the cluster containing metadata for topics with valid metadata
     * @param unavailableTopics topics which are non-existent or have one or more partitions whose
     *                          leader is not known
     * @param now               current time in milliseconds
     */
    public synchronized void update(Cluster newCluster, Set<String> unavailableTopics, long now) {
        Objects.requireNonNull(newCluster, "cluster should not be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener : listeners)
            listener.onMetadataUpdate(newCluster, unavailableTopics);

        String previousClusterId = cluster.clusterResource().clusterId();

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(newCluster);
        } else {
            this.cluster = newCluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!newCluster.isBootstrapConfigured()) {
            String newClusterId = newCluster.clusterResource().clusterId();
            if (newClusterId == null ? previousClusterId != null : !newClusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", newClusterId);
            clusterResourceListeners.onUpdate(newCluster.clusterResource());
        }

        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
        this.lastRefreshMs = now;
        this.authenticationException = authenticationException;
        if (authenticationException != null)
            this.notifyAll();
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     *
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * "Close" this metadata instance to indicate that metadata updates are no longer possible. This is typically used
     * when the thread responsible for performing metadata updates is exiting and needs a way to relay this information
     * to any other thread(s) that could potentially wait on metadata update to come through.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
        this.notifyAll();
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        /**
         * Callback invoked on metadata update.
         *
         * @param cluster           the cluster containing metadata for topics with valid metadata
         * @param unavailableTopics topics which are non-existent or have one or more partitions whose
         *                          leader is not known
         */
        void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
    }

    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        requestUpdate();
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        Node controller = null;
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (!partitionInfoList.isEmpty()) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
            controller = cluster.controller();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics, controller);
    }
}
