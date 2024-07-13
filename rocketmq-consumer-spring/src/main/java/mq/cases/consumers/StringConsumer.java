package mq.cases.consumers;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.topic}", consumerGroup = "string_consumer", selectorExpression = "${demo.rocketmq.tag}", tlsEnable = "${demo.rocketmq.tlsEnable}")
public class StringConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        System.out.printf("------- StringConsumer received: %s \n", message);
    }
}
