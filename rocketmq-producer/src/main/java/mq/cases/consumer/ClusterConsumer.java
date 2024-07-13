package mq.cases.consumer;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMachineRoomNearby;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class ClusterConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dw_test_consumer_6");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setClientIP("MR1-" + RemotingUtil.getLocalAddress());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("m_t_t", "*");
        AllocateMessageQueueStrategy strategy = new AllocateMessageQueueAveragely();
        AllocateMachineRoomNearby.MachineRoomResolver resolver = new AllocateMachineRoomNearby.MachineRoomResolver() {
            @Override
            public String brokerDeployIn(MessageQueue messageQueue) {
                return messageQueue.getBrokerName().split("-")[0];
            }

            @Override
            public String consumerDeployIn(String clientID) {
                return clientID.split("\\.")[0];
            }
        };
        consumer.setAllocateMessageQueueStrategy(new AllocateMachineRoomNearby(strategy,
                resolver));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                System.out.printf("%s recv msgs: %s%n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
