package org.apache.example.learn2.entity;

import java.io.Serializable;

/**
 * 消息对象
 *
 * @author
 * @date 2019/6/15
 */

public class KafkaMessage implements Serializable {
    private String id;
    // 消息对象
    private Message message;
    // .....其他属性

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public class Message implements Serializable {

        public Long getValue() {
            return value;
        }

        public void setValue(Long value) {
            this.value = value;
        }

        private Long value;
        // ....其他属性
    }

    public Message builder() {
        return new Message();
    }

}
