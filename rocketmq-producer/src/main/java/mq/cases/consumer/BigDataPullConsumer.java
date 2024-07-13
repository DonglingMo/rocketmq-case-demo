package mq.cases.consumer;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class BigDataPullConsumer {
    private final ExecutorService executorService =
            new ThreadPoolExecutor(30, 30, 0L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000),
                    new DefaultThreadFactory("BigDataPullConsumer-"));

    private final ExecutorService pullTaskExecutor =
            new ThreadPoolExecutor(1, 1, 0L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(10),
                    new DefaultThreadFactory("pullBatch-"));

    private String consumerGroup;
    private String nameserverAddr;
    private String topic;
    private String filter;
    private MessageListener messageListener;
    private DefaultMQProducer retryMQProducer;
    private PullBatchTask pullBatchTask;

    public BigDataPullConsumer(String consumerGroup, String nameserverAddr, String topic, String filter) {
        this.consumerGroup = consumerGroup;
        this.nameserverAddr = nameserverAddr;
        this.topic = topic;
        this.filter = filter;
        initRetryMQProducer();
    }

    private void initRetryMQProducer() {
        this.retryMQProducer = new DefaultMQProducer(consumerGroup + "-retry");
        this.retryMQProducer.setNamesrvAddr(nameserverAddr);
        try {
            this.retryMQProducer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public void start() {
        this.pullBatchTask = new PullBatchTask(nameserverAddr, topic, filter, messageListener);
        pullTaskExecutor.submit(pullBatchTask);
    }

    public void stop() {
        while (pullBatchTask.isRunning()) {
            try {
                Thread.sleep(1 * 1000L);
            } catch (InterruptedException e) {

            }
        }
        pullBatchTask.stop();
        pullTaskExecutor.shutdown();
        executorService.shutdown();
        try {
            while (executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                retryMQProducer.shutdown();
                break;
            }
        } catch (InterruptedException e) {}

    }

    public static interface MessageListener {
        boolean consume(List<MessageExt> msgs);
    }

    class PullBatchTask implements Runnable {
        private volatile boolean running = true;
        DefaultLitePullConsumer consumer;
        String consumerGroup;
        String nameserverAddr;
        String topic;
        String filter;
        private MessageListener messageListener;

        public PullBatchTask(String nameserverAddr, String topic, String filter, BigDataPullConsumer.MessageListener messageListener) {
            this.nameserverAddr = nameserverAddr;
            this.topic = topic;
            this.filter = filter;
            this.messageListener = messageListener;
            init();
        }

        private void init() {
            consumer = new DefaultLitePullConsumer(consumerGroup);
            consumer.setNamesrvAddr(nameserverAddr);
            consumer.setAutoCommit(true);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            try {
                consumer.subscribe(topic, filter);
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void stop() {
            this.running = false;
            consumer.shutdown();
        }

        public boolean isRunning() {
            return running;
        }

        @Override
        public void run() {
            running = true;
            long startTime = System.currentTimeMillis() - 5 * 1000;
            int notFoundMsgCount = 0;
            while (running) {
                try {
                    List<MessageExt> messageExts = consumer.poll();
                    if (messageExts != null && ! messageExts.isEmpty()) {
                        notFoundMsgCount = 0;
                        try {
                            executorService.submit(new ExecuteTask(messageExts, messageListener));
                        } catch (RejectedExecutionException e) {
                            boolean retry = true;
                            while (retry) {
                                try {
                                    Thread.sleep(5 * 1000L);
                                    executorService.submit(new ExecuteTask(messageExts, messageListener));
                                    retry = false;
                                } catch (RejectedExecutionException e2) {
                                    retry = true;
                                }
                            }
                            MessageExt last = messageExts.get(messageExts.size() - 1);
                            if (last.getStoreTimestamp() > startTime) {
                                consumer.pause(buildMessageQueues(last));
                            }
                        }
                    } else {
                        notFoundMsgCount++;
                    }
                    if (notFoundMsgCount > 5) {
                        break;
                    }
                }  catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            running = false;
        }

        public Set<MessageQueue> buildMessageQueues(MessageExt msg) {
            Set<MessageQueue> queues = new HashSet<>();
            MessageQueue queue = new MessageQueue(msg.getTopic(), msg.getBrokerName(), msg.getQueueId());
            queues.add(queue);
            return queues;
        }
    }
    class ExecuteTask implements Runnable {
        private List<MessageExt> msgs;
        private MessageListener messageListener;

        public ExecuteTask(List<MessageExt> msgs, MessageListener messageListener) {
            this.msgs = msgs.stream().filter((MessageExt msg) -> msg.getReconsumeTimes() <= 16).collect(Collectors.toList());
            this.messageListener = messageListener;
        }

        @Override
        public void run() {
            try {
                messageListener.consume(msgs);
            } catch (Exception e) {
                try {
                    for (MessageExt msg : msgs) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        retryMQProducer.send(msg);
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}


