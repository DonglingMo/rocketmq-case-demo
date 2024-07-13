package mq.cases;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
import java.util.List;

@SpringBootApplication
public class ConsumerApplication implements CommandLineRunner {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Resource(name = "extRocketMQTemplate")
    private RocketMQTemplate extRocketMQTemplate;

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        //This is an example of pull consumer using rocketMQTemplate.
        List<String> messages = rocketMQTemplate.receive(String.class);
        System.out.printf("receive from rocketMQTemplate, messages=%s %n", messages);

        //This is an example of pull consumer using extRocketMQTemplate.
        messages = extRocketMQTemplate.receive(String.class);
        System.out.printf("receive from extRocketMQTemplate, messages=%s %n", messages);
    }
}