package mq.cases;

import org.apache.rocketmq.spring.annotation.ExtRocketMQTemplateConfiguration;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

@ExtRocketMQTemplateConfiguration(nameServer = "${demo.rocketmq.extNameServer}", tlsEnable = "${demo.rocketmq.ext.useTLS}", instanceName = "pztest33")
public class ExtRocketMQTemplate extends RocketMQTemplate {
}