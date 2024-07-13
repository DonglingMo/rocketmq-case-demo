package mq.cases.sender;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class KeyChoose {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("key_topic");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        //order
        Order order = new Order();
        order.setId(1002L);
        order.setOrderId("1234567890");
        order.setStatus(0);
        Message message = new Message("key_test", null, order.getOrderId() + " ODS0002", JSON.toJSONString(order).getBytes());
        System.out.printf("%s%n", producer.send(message));
        producer.shutdown();
    }
}



