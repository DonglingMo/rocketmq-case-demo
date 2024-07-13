package mq.cases.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class PullConsumer {
    private static volatile  boolean isRunning = true;
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(()  -> {
            try {
                DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("dw_pull_consumer");
                consumer.setNamesrvAddr("127.0.0.1:9876");
                consumer.start();
                Map<MessageQueue, Long> offsetTable = new HashMap<>();
                // 获取主题的所有队列消息
                Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues("topic_test");
                if (messageQueueSet != null && !messageQueueSet.isEmpty()) {
                    boolean notFound = false;
                    while (isRunning) {
                        if (notFound) {
                            Thread.sleep(1000L);
                        }
                        for (MessageQueue queue : messageQueueSet) {
                            // 从broker依次拉取消息
                            PullResult pull = consumer.pull(queue, "*", dicivedPullOffseta(offsetTable, queue, consumer), 3000);
                            System.out.println("pull status:" + pull.getPullStatus());
                            switch (pull.getPullStatus()) {
                                case FOUND:
                                    doSomething(pull.getMsgFoundList());
                                    break;
                                    case NO_MATCHED_MSG:
                                        break;
                                case NO_NEW_MSG:
                                case OFFSET_ILLEGAL:
                                    notFound = true;
                                    break;
                                default:
                                    continue;
                            }
                            // 更新队列偏移，每5s一次
                            consumer.updateConsumeOffset(queue, pull.getNextBeginOffset());
                        }
                        System.out.println(consumer.fetchMessageQueuesInBalance("topic_test").isEmpty());
                    }
                } else {
                    System.out.println("queue is empty");
                }
                consumer.shutdown();
                System.out.println("consumer shutdown");
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
        thread.start();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            countDownLatch.await(120 * 1000, TimeUnit.MICROSECONDS);
        } finally {
            isRunning = false;
        }
    }

    private static void doSomething(List<MessageExt> msgFoundList) {
        System.out.println("msgFoundList:" + msgFoundList.size());
    }

    private static long dicivedPullOffseta(Map<MessageQueue, Long> offsetTable, MessageQueue queue, DefaultMQPullConsumer consumer) throws MQClientException {
        long offset = consumer.fetchConsumeOffset(queue, false);
        if (offset < 0) {
            offset = 0;
        }
        System.out.println("offset:" + offset);
        return offset;
    }
}
