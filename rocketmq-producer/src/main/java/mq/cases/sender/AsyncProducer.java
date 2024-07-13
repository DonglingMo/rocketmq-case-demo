package mq.cases.sender;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class AsyncProducer {
    public static void main(String[] args) throws InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("async_producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setUnitName("ClusterA");
        try {
            producer.start();
            Message message = new Message("Topic_Test", "Hello World Async".getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(3000L);
        producer.shutdown();
    }
}
