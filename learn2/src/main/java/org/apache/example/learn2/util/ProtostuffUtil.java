package org.apache.example.learn2.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author maochao
 * @since 2019/7/8 18:00
 */
public class ProtostuffUtil {
    private static final Objenesis objenesis = new ObjenesisStd(true);
    private static final ConcurrentMap<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap<>();
    private static ThreadLocal<LinkedBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> LinkedBuffer.allocate());

    public static <T> byte[] serialize(T obj) {
        Schema<T> schema = getSchema((Class<T>)obj.getClass());
        // 默认分配512
        LinkedBuffer buf = bufferThreadLocal.get();
        try {
            // 实现object->byte[]
            return ProtostuffIOUtil.toByteArray(obj, schema, buf);
        } finally {
            buf.clear();
        }
    }

    private  static<T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>)schemaCache.get(clazz);
        if (schema == null) {
            // 把可序列化的字段封装到Schema
            Schema<T> newSchema = RuntimeSchema.createFrom(clazz);
            schema = (Schema<T>)schemaCache.putIfAbsent(clazz, newSchema);
            if (schema == null) {
                schema = newSchema;
            }
        }
        return schema;
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        // java原生实例化必须调用constructor. 故使用objenesis
        T object = objenesis.newInstance(clazz);
        Schema<T> schema = getSchema(clazz);
        // 反序列化源码跟踪入口
        ProtostuffIOUtil.mergeFrom(bytes, object, schema);
        return object;
    }

}
