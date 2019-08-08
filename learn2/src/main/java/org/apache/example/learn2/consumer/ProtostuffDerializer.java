package org.apache.example.learn2.consumer;

import org.apache.example.learn2.entity.KafkaMessage;
import org.apache.example.learn2.util.ProtostuffUtil;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author maochao
 * @since 2019/7/8 19:29
 */
public class ProtostuffDerializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return ProtostuffUtil.deserialize(data, KafkaMessage.class);
    }

    @Override
    public void close() {

    }
}
