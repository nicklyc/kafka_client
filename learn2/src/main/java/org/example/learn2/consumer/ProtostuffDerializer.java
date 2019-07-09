package org.example.learn2.consumer;

import org.apache.kafka.common.serialization.Deserializer;
import org.example.learn2.entity.KafkaMessage;
import org.example.learn2.util.ProtostuffUtil;

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
