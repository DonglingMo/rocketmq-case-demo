package mq.cases.consumer;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class LitePullConsumer {
    private static volatile boolean running = true;
    public static void main(String[] args) throws MQClientException {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("dw_lite_pull_consumer");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("lite_topic_test", "*");
        //自动提交
        consumer.setAutoCommit(true);
        consumer.start();
        try {
            while (running) {
                List<MessageExt> poll = consumer.poll();
                doSomething(poll);

            }
        } finally {
            consumer.shutdown();
        }

    }

    private static void doSomething(List<MessageExt> poll) {
        System.out.printf("%s%n", poll);
    }
}
