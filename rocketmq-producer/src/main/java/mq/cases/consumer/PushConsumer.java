package mq.cases.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class PushConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dw_test_consumer_2");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 最早消息
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("test_topic", "*");
        // 平均分配
        consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragelyByCircle());
        // 消息监听回调
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                System.out.printf("%s recv msgs: %s%n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Throwable e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
    }
}
