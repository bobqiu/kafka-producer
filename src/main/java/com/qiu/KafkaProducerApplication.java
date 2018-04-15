package com.qiu;

import com.qiu.sender.MessageSender;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaProducerApplication {
    public static void main(String[] args) {
        //SpringApplication.run(KafkaProducerApplication.class, args);
        ConfigurableApplicationContext context = SpringApplication.run(KafkaProducerApplication.class, args);

        MessageSender messageSender = context.getBean(MessageSender.class);

        for (int i = 0; i < 300000; i++) {
            //调用消息发送类中的消息发送方法
            messageSender.send("test_topic", "this is test " + String.valueOf(i));

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
