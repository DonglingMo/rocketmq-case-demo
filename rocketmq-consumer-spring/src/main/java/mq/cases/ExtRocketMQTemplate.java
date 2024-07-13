package mq.cases;

import org.apache.rocketmq.spring.annotation.ExtRocketMQConsumerConfiguration;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

@ExtRocketMQConsumerConfiguration(topic = "${demo.rocketmq.topic}", group = "string_consumer", tlsEnable = "${demo.ext.consumer.tlsEnable}")
public class ExtRocketMQTemplate extends RocketMQTemplate {
}