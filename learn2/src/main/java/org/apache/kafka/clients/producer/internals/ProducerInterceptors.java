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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * 拦截器包装类，持有一组拦截器链 List<ProducerInterceptor<K, V>> interceptors
 * 有顺序的
 * onSend（），方法就是按照配置顺序依次执行每个自定义拦截器重写的onsend方法，
 * 这个方法，如果有中途的拦截器抛出异常，那就继续执行下一个拦截器进行处理，该拦截器失效。
 * onAcknowledgement（），就是收到ack的回调方法，每个拦截器都会执行，
 * onSendError（），发送失败的回调方法，每个拦截器都会执行
 *
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    private final List<ProducerInterceptor<K, V>> interceptors;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * 拦截器处理方法，
     *
     * @param record the record from client
     * @return producer record to send to topic/partition
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        ProducerRecord<K, V> interceptRecord = record;
        //遍历拦截器，依次执行onSend方法
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptRecord = interceptor.onSend(interceptRecord);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                // be careful not to throw exception from here
                if (record != null)
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(),
                        record.partition(), e);
                else
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails
     * before it gets sent to the server. This method calls
     * {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). If an error occurred,
     *            metadata will only contain valid topic and maybe partition.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.onAcknowledgement(metadata, exception);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend (ProducerRecord)}
     * method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)} method for
     * each interceptor
     *
     * @param record The record from client
     * @param interceptTopicPartition The topic/partition for the record if an error occurred after partition gets
     *            assigned; the topic part of interceptTopicPartition is the same as in record.
     * @param exception The exception thrown during processing of this record.
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    interceptor.onAcknowledgement(null, exception);
                } else {
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = new TopicPartition(record.topic(),
                            record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
                    }
                    interceptor.onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                        RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1), exception);
                }
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * Closes every interceptor in a container.
     */
    @Override
    public void close() {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
