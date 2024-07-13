package mq.cases.consumers;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(nameServer = "${demo.rocketmq.myNameServer}", topic = "${demo.rocketmq.topic}", consumerGroup = "string_consumer_newns")
public class StringConsumerNewNS implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        System.out.printf("------- StringConsumerNewNS received: %s \n", message);
    }
}
