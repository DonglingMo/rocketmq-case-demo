package mq.cases.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class SimpleConsumer {
    public static void main(String[] args) throws MQClientException {
        // 创建消费者实例，并设置消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("first_consumer_group");
        // 设置 Name Server 地址，此处为示例，实际使用时请替换为真实的 Name Server 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅指定的主题和标签（* 表示所有标签）
        consumer.subscribe("TestTopic", "*");

        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("Received message: " + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者
//        consumer.setConsumeTimeout(60000);
//        DefaultChannelId.newInstance();
        consumer.start();
        System.out.println("Consumer started.");
    }
}
