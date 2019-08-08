package org.example.learn2.spring.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * future 的回调 生产者发送消息回调
 *
 * @author maochao
 * @since 2019/8/6 20:29
 */
public class ProducerCallback implements ListenableFutureCallback<SendResult<Integer, String>> {
    private static final Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override
    public void onFailure(Throwable throwable) {
        logger.info(throwable.getMessage(), throwable);
        // 返回失败处理
    }

    @Override
    public void onSuccess(SendResult<Integer, String> result) {
        logger.info("发送成功");
        logger.info(result.toString());

        // 返回成功处理
    }
}
