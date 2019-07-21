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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 * 写数据容器封装
 */
public class ByteBufferSend implements Send {
    /**
     * 发送地址
     */
    private final String destination;
    /**
     * 总长度，所有buffer的总大小
     */
    private final int size;
    /**
     * 发送buffer[size|data]
     */
    protected final ByteBuffer[] buffers;
    /**
     * 还剩余的长度
     */
    private int remaining;
    /**
     * 是否挂起
     */
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        this.destination = destination;
        this.buffers = buffers;
        //计算总长度
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
      //将内容写进buffer
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = TransportLayers.hasPendingWrites(channel);
        return written;
    }
}
