package org.example.learn2.consumer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.apache.kafka.common.serialization.Deserializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * kryo解码器
 *
 * @author
 * @date 2019/6/15
 */
public class KryoDeserializer implements Deserializer<Object> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        try {
            if (bytes == null) {
                return null;
            } else {
                return Deserializer.deserialize(bytes);
            }
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    @Override
    public void close() {

    }


    private static class Deserializer {
        private static Object deserialize(byte[] bytes) {
            Kryo kryo = getKryo();
            Input input = new Input();
            ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
            input.setInputStream(stream);
            Object o = kryo.readClassAndObject(input);
            input.close();
            return o;
        }

        private static Kryo getKryo() {
            Kryo kryo = new Kryo();
            StdInstantiatorStrategy stdInstantiatorStrategy = new StdInstantiatorStrategy();
            DefaultInstantiatorStrategy defaultInstantiatorStrategy = new DefaultInstantiatorStrategy(stdInstantiatorStrategy);
            kryo.setInstantiatorStrategy(defaultInstantiatorStrategy);
            kryo.setReferences(false);
            kryo.setRegistrationRequired(false);
            kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
            return kryo;
        }
    }
}
