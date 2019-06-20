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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public final class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {}

    /**
     * 获取kafka 集群的地址列表
     * 
     * @param urls
     * @return List<InetSocketAddress>
     */
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String url : urls) {
            if (url != null && !url.isEmpty()) {
                try {
                    String host = getHost(url);
                    Integer port = getPort(url);
                    if (host == null || port == null)
                        throw new ConfigException(
                            "Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

                    InetSocketAddress address = new InetSocketAddress(host, port);

                    if (address.isUnresolved()) {
                        log.warn("Removing server {} from {} as DNS resolution failed for {}", url,
                            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
                    } else {
                        addresses.add(address);
                    }
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(
                        "Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.isEmpty())
            throw new ConfigException(
                "No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    public static void closeQuietly(Closeable c, String name, AtomicReference<Throwable> firstException) {
        if (c != null) {
            try {
                c.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close " + name, t);
            }
        }
    }

    /**
     * 创建一个 ChannelBuilder
     * 
     * @param config client configs
     * @return configured ChannelBuilder based on the configs.
     */
    public static ChannelBuilder createChannelBuilder(AbstractConfig config) {
        /**读取security.protocol 配置，客户端与broker之间的安全协议 四种协议：
         *
         * PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
         *
         * 具体查看SecurityProtocol 说明
         *
         */
        SecurityProtocol securityProtocol =
            SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));

        //读取sasl.mechanism 配置
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, null,
            clientSaslMechanism, true);
    }
}
