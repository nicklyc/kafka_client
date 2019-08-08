package org.apache.example.learn2.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 生产者拦截器
 *
 * @author
 * @date 2019/6/15
 */
public class Interceptor implements ProducerInterceptor {


    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        Headers headers = record.headers();
        HashMap<String, String> map = new HashMap<>();
        map.put("token", UUID.randomUUID().toString());
        String time = String.valueOf(System.currentTimeMillis());
        Header header = new RecordHeader("timeStamp",time.getBytes());

       //两种方式添加header
        headers.add(header);
        headers.add("token", UUID.randomUUID().toString().getBytes());

        //ProducerRecord 其他属性改造.....
        //
        System.out.println("消息异步落地...");

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if (exception!=null){
           //发送消息失败
            System.out.println("消息失败同一记录处理  >>"+exception);
        }else{
           //发送消息成功
            System.out.println("消息成功同一处理");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        /**
         * 拦截用户自定义的配置
         */
        System.out.println("自定义配置");
        System.out.println(configs);
    }
}
