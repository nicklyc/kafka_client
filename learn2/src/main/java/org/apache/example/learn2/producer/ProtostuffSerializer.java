package org.apache.example.learn2.producer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.example.learn2.util.ProtostuffUtil;

import java.util.Map;

/**
 * 
 * ProtostuffSerializer 编码器
 * 
 * @author maochao
 * @since 2019/7/8 17:58
 * 
 */
public class ProtostuffSerializer implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return ProtostuffUtil.serialize(data);
    }

    @Override
    public void close() {

    }
}
