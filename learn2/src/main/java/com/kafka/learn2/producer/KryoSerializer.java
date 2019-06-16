package com.kafka.learn2.producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.kafka.learn2.entity.KafkaMessage;
import org.apache.kafka.common.serialization.Serializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * kryo编码器
 *
 * @author
 * @date 2019/6/15
 */
public class KryoSerializer implements Serializer {




    private Kryo kryo;

    @Override
    public void configure(Map configs, boolean isKey) {
        kryo = new Kryo();
        DefaultInstantiatorStrategy defaultInstantiatorStrategy = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setInstantiatorStrategy(defaultInstantiatorStrategy);
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
    }

    @Override
    public byte[] serialize(String topic, Object data) {

        // try {
        if (data == null) {
            return null;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] serialize = Serializer.serialize(data, outputStream);
        return serialize;
        /*} catch (Exception e) {
            return null;
        }*/

    }


    @Override
    public void close() {

    }

     private static class Serializer{

         private static byte[] serialize(Object object, ByteArrayOutputStream outputStream) {
             Kryo kryo =getKryo();
             Output output = new Output(1024, 102400);
             output.setOutputStream(outputStream);
             kryo.writeClassAndObject(output, object);
             output.close();
             return outputStream.toByteArray();
         }

         private  static  Kryo getKryo() {
             Kryo kryo = new Kryo();
             DefaultInstantiatorStrategy defaultInstantiatorStrategy = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
             kryo.setInstantiatorStrategy(defaultInstantiatorStrategy);
             kryo.setReferences(false);
             kryo.setRegistrationRequired(false);
             kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
             return kryo;
         }
    }

}
