package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Auther: Lenovo
 * @Date: 2020/4/17 10:49
 * @Description:
 */
public class ProductTest {

    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_group_a");
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        for (int i = 0; i < 5; i++) {
            Thread.sleep(2000);
            // topic tags content is byte[]
            Message message = new Message("TopicTest","TagA","hello word ".getBytes());

            SendResult send = producer.send(message);

            System.out.printf("%s%n：",send);
        }
        producer.shutdown();
    }
}
