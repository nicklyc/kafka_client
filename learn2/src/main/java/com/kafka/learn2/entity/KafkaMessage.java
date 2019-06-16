package com.kafka.learn2.entity;

import com.esotericsoftware.kryo.DefaultSerializer;

import java.io.Serializable;
import java.util.List;

/**
 * 消息对象
 *
 * @author
 * @date 2019/6/15
 */
public class KafkaMessage implements Serializable {
    private String id;
    private Long value;
    //.....其他属性

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }


}
