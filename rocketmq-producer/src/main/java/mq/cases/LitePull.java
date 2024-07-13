package mq.cases;

import mq.cases.consumer.BigDataPullConsumer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LitePull {
    public static void main(String[] args) {
        String consumerGroup = "dw_test_lite_pull_group";
        String namesrvAddr = "localhost:9876";
        String topic = "dw_topic";
        String filter = "*";
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new DefaultThreadFactory("main-scheduled-thread-"));
        executorService.scheduleWithFixedDelay((Runnable) () -> {
            BigDataPullConsumer consumer = new BigDataPullConsumer(consumerGroup, namesrvAddr, topic, filter);
           consumer.registerMessageListener(msgs -> true);
           consumer.start();
           consumer.stop();
        }, 1000, 30 * 1000, TimeUnit.MILLISECONDS);

        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(10, TimeUnit.MINUTES);
            executorService.shutdown();
        } catch (InterruptedException e) {
        }
    }
}
