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
package org.apache.kafka.common.network;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.Objects;

/**
 * kafka 网络连接通道，实际上是持有的SocketChannel
 */
public class KafkaChannel {
    /**
     * Mute States for KafkaChannel:
     * <ul>
     * <li> NOT_MUTED: Channel is not muted. This is the default state. </li>
     * <li> MUTED: Channel is muted. Channel must be in this state to be unmuted. </li>
     * <li> MUTED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted and SocketServer has not sent a response
     * back to the client yet (acks != 0) or is currently waiting to receive a
     * response from the API layer (acks == 0). </li>
     * <li> MUTED_AND_THROTTLED: (SocketServer only) Channel is muted and throttling is in progress due to quota
     * violation. </li>
     * <li> MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted, throttling is in progress,
     * and a response is currently pending. </li>
     * </ul>
     */
    public enum ChannelMuteState {
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }

    ;

    /**
     * Socket server events that will change the mute state:
     * <ul>
     * <li> REQUEST_RECEIVED: A request has been received from the client. </li>
     * <li> RESPONSE_SENT: A response has been sent out to the client (ack != 0) or SocketServer has heard back from
     * the API layer (acks = 0) </li>
     * <li> THROTTLE_STARTED: Throttling started due to quota violation. </li>
     * <li> THROTTLE_ENDED: Throttling ended. </li>
     * </ul>
     * <p>
     * Valid transitions on each event are:
     * <ul>
     * <li> REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING </li>
     * <li> RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED </li>
     * <li> THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING </li>
     * <li> THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING </li>
     * </ul>
     */
    public enum ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    }

    ;
    /**
     * nodeId
     */
    private final String id;
    /**
     * 读写数据
     */
    private final TransportLayer transportLayer;
    /**
     * 认证
     */
    private final Authenticator authenticator;
    // Tracks accumulated network thread time. This is updated on the network thread.
    // The values are read and reset after each response is sent.
    private long networkThreadTimeNanos;
    /**
     * 最大读取size，读取消息头中的消息长度大于该值，将会抛出@InvalidReceiveException
     * 详细见@NetworkReceive##readFrom
     *
     */
    private final int maxReceiveSize;
    /**
     * 内存池
     */
    private final MemoryPool memoryPool;
    /**
     * 读取数据容器
     */
    private NetworkReceive receive;
    /**
     * 请求对象
     */
    private Send send;
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private ChannelMuteState muteState;
    /**
     * Channel 连接状态的标记
     */
    private ChannelState state;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize, MemoryPool memoryPool) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.networkThreadTimeNanos = 0L;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.disconnected = false;
        this.muteState = ChannelMuteState.NOT_MUTED;
        this.state = ChannelState.NOT_CONNECTED;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator, receive);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     * 获取身份认证信息
     */
    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, {@link TransportLayer#handshake()} performs
     * authentication. For SASL, authentication is performed by {@link Authenticator#authenticate()}.
     *握手认证
     * PLAINTEXT 协议，握手和认证都没有具体实现，不需要身份认证
     * SSL会进行用户认证
     */
    public void prepare() throws AuthenticationException, IOException {
        try {
            //握手，认证没有完成
            if (!transportLayer.ready())
                //进行握手
                transportLayer.handshake();
            //认证
            if (transportLayer.ready() && !authenticator.complete())
                authenticator.authenticate();
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e);
            throw e;
        }
        if (ready())
            state = ChannelState.READY;
    }

    public void disconnect() {
        disconnected = true;
        transportLayer.disconnect();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    /**
     * 检查连接是否建立完成
     * @return
     * @throws IOException
     */
    public boolean finishConnect() throws IOException {
        boolean connected = transportLayer.finishConnect();
        if (connected)
            //标记状态，连接建立完成，如果不是ready,则是在认证中
            state = ready() ? ChannelState.READY : ChannelState.AUTHENTICATE;
        return connected;
    }

    /**
     *SOCKET是否建立连接
     * 与finishConnect的差别就是，当阻塞模式下，当socket打开成功
     * 该方法则为true，但是可能并未真正的连接
     * @return
     */
    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    /**
     * 获取该channel上的SelectionKey
     * @return
     */
    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * Unmute the channel. The channel can be unmuted only if it is in the MUTED state. For other muted states
     * (MUTED_AND_*), this is a no-op.
     *
     * @return Whether or not the channel is in the NOT_MUTED state after the call
     */
    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMute() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    /**
     * Channel是否ready,
     * PlaintextTransportLayer 默认建立连接就是ready
     * 采用PLAINTEXT 协议不用认证，默认是true
     * @return
     */
    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     * <p>
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     * 获取远程的Ip地址
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }


    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 设置Send 数据
     * @param send
     */
    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        this.send = send;
        //添加到interestOps
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     *网络读
     * @return
     * @throws IOException
     */
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            //构造一个NetworkReceive数据读取容器
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }
        //读取数据
        receive(receive);
        //数据读结束
        if (receive.complete()) {
            //重置消息体buffer指针
            receive.payload().rewind();
            result = receive;
            receive = null;
        } else if (receive.requiredMemoryAmountKnown() && !receive.memoryAllocated() && isInMutableState()) {
            //pool must be out of memory, mute ourselves.
            mute();
        }
        return result;
    }

    /**
     * 网络写
     * 发送完成则返回send,
     * 没有发送完成返回null
     * @return
     * @throws IOException
     */
    public Send write() throws IOException {
        Send result = null;
        //发送
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * Returns accumulated network thread time for this channel and resets
     * the value to zero.
     */
    public long getAndResetNetworkThreadTimeNanos() {
        long current = networkThreadTimeNanos;
        networkThreadTimeNanos = 0;
        return current;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        //这里使用的NetWorkSend，网络写，将数据写入SOCKET
        send.writeTo(transportLayer);
        //写结束，buffer内容写完表示结束，
        if (send.completed())
            //移除selectorKey的感兴趣集合写事件，
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
