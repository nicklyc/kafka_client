/*
package com.kafka.learn2.producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.kafka.learn2.entity.KafkaMessage;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
public class Main {

    public static void main(String... args) throws FileNotFoundException {
        Main main = new Main();
        main.testKryo5();
    }

    private void testKryo5() throws FileNotFoundException {
        KafkaMessage message = new KafkaMessage();
        message.setId("123");
        message.setValue(12123L);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = this.serialize(message, outputStream);

        KafkaMessage deserialize = (KafkaMessage) this.deserialize(bytes);
        System.out.println(">>>>>>"+deserialize.getId() + 1L);
    }



    private byte[] serialize(Object object, ByteArrayOutputStream outputStream) throws FileNotFoundException {
        Kryo kryo = this.getKryo();
        Output output = new Output(1024, 102400);
        output.setOutputStream(outputStream);
        kryo.writeClassAndObject(output, object);
        output.close();
        return outputStream.toByteArray();
    }

    private Object deserialize(byte[] bytes) {
        Kryo kryo = this.getKryo();
        Input input = new Input(102400);
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        input.setInputStream(stream);
        Object o = kryo.readClassAndObject(input);
        input.close();
        return o;
    }



    private Kryo getKryo() {
        Kryo kryo = new Kryo();
        DefaultInstantiatorStrategy defaultInstantiatorStrategy = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setInstantiatorStrategy(defaultInstantiatorStrategy);
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        return kryo;
    }
}
*/
