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
package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the coordinator 心跳记录
 */
public final class Heartbeat {

    /**
     * session 超时时间-->session.timeout.ms 配置 10s
     */
    private final int sessionTimeoutMs;
    /**
     * 心跳间隔时间 ---> heartbeat.interval.ms 默认值3000 毫秒
     */
    private final int heartbeatIntervalMs;
    /**
     * poll 最大时间 -->max.poll.interval.ms 默认值300000 毫秒 5分钟
     */
    private final int maxPollIntervalMs;
    /**
     * 重试时间 ---> retry.backoff.ms 默认值 100毫秒
     */
    private final long retryBackoffMs;
    /**
     * 上一次发送心跳时间
     */
    private volatile long lastHeartbeatSend;
    /**
     * 上一次心跳返回时间。
     */
    private long lastHeartbeatReceive;
    /**
     * 最后请求时间
     */
    private long lastSessionReset;
    /**
     * 最后
     */
    private long lastPoll;
    /**
     * 心跳是否成功
     */
    private boolean heartbeatFailed;

    public Heartbeat(int sessionTimeoutMs, int heartbeatIntervalMs, int maxPollIntervalMs, long retryBackoffMs) {
        if (heartbeatIntervalMs >= sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxPollIntervalMs = maxPollIntervalMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * poll 重置最后心跳时间为now
     * 
     * @param now
     */
    public void poll(long now) {
        this.lastPoll = now;
    }

    /**
     * 记录发送心跳时间
     * 
     * @param now
     */
    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
        this.heartbeatFailed = false;
    }

    /**
     * 
     */
    public void failHeartbeat() {
        this.heartbeatFailed = true;
    }

    /**
     * 记录心跳接收时间
     * 
     * @param now
     */
    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    /**
     * 是否应该发送心跳
     * 
     * 心跳时间达到3s ，重试内达到100毫秒 ，
     * 
     * @param now
     * @return
     */
    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }

    /**
     * 记录最后心跳发送时间
     * 
     * @return
     */
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    /**
     * 心跳倒计时
     * 
     * 最后一次请求时间 距离当前时间的间隔值是否超过了 心跳间隔时间
     * 心跳间隔时间 在心跳失败期间使用重试时间 否则使用心跳间隔时间
     * 
     * @param now
     * @return
     */
    public long timeToNextHeartbeat(long now) {
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);
        final long delayToNextHeartbeat;
        // 如果心跳失败，采用重试间隔时间
        if (heartbeatFailed)
            delayToNextHeartbeat = retryBackoffMs;
        else
            // 否则使用 心跳间隔时间
            delayToNextHeartbeat = heartbeatIntervalMs;

        if (timeSinceLastHeartbeat > delayToNextHeartbeat)
            return 0;
        else
            return delayToNextHeartbeat - timeSinceLastHeartbeat;
    }

    /**
     * 回话是否超时
     *
     * 10s 没有心跳则认为超时 可以配置
     * 
     * @param now
     * @return
     */
    public boolean sessionTimeoutExpired(long now) {
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > sessionTimeoutMs;
    }

    public long interval() {
        return heartbeatIntervalMs;
    }

    /**
     * 重置超时
     * 
     * @param now
     */
    public void resetTimeouts(long now) {
        this.lastSessionReset = now;
        this.lastPoll = now;
        this.heartbeatFailed = false;
    }

    public boolean pollTimeoutExpired(long now) {
        return now - lastPoll > maxPollIntervalMs;
    }

    public long lastPollTime() {
        return lastPoll;
    }

}