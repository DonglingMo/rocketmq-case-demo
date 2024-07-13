package mq.cases.sender;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.Serializable;

public class QueueChoose {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producerGroupChooseQueue");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        //order
        Order order = new Order();
        order.setId(1001L);
        order.setOrderId("1234567890");
        order.setStatus(0);
        SendResult sendResult = sendMsg(producer, order);
        System.out.printf("%s%n", sendResult);
        order.setStatus(1);
        // 订单状态变更，重新发送消息
        SendResult sendResult2 = sendMsg(producer, order);
        System.out.printf("%s%n", sendResult2);
        producer.shutdown();
    }

    private static SendResult sendMsg(DefaultMQProducer producer, Order order) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Message message = new Message("order_topic", null, order.getOrderId(), JSON.toJSONString(order).getBytes());
        return producer.send(message, (mqs, msg, arg) -> {
            if (msg == null || mqs.isEmpty()) {
                return null;
            }
            int index = Math.abs(arg.hashCode()) % mqs.size();
            return mqs.get(Math.max(index, 0));
        }, order.getOrderId());
    }
}

class Order implements Serializable {
    private Long id;
    private String orderId;
    private Integer status;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
