package org.example.learn2.spring.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.example.learn2.spring.producer.MyproducerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * 消费者异常处理
 * 
 * @author maochao
 * @since 2019/8/7 15:05
 */
@Component
public class ConsumerErrorHander implements KafkaListenerErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerErrorHander.class);

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) throws Exception {
        logger.info("message     :  "+message.toString());
        logger.info("exception   :  "+exception.getMessage(),exception);
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) throws Exception {

        logger.info("消费出现异常     : 异常处理 ");
        logger.info("message     :  "+message.toString());
        logger.info("consumer     :  "+consumer.toString());
        logger.info("exception   :  "+exception.getMessage(),exception);
        return null;
    }
}
