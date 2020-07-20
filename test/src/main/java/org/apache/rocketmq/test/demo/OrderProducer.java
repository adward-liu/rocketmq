package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class OrderProducer {

    public static void main(String[] args) throws MQClientException,
            InterruptedException {

        /**
         * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例
         * 注意：ProducerGroupName需要由应用来保证唯一
         * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键
         * 因为服务器会回查这个Group下的任意一个Producer
         */
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("172.20.1.186:9876");
        producer.setInstanceName("Producer");
        /**
         * Rocket默认开启了VIP通道，VIP通道端口为10911-2=10909。若Rocket服务器未启动端口10909，则报connect to <> failed
         */
        producer.setVipChannelEnabled(false);

        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();

        /**
         * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高
         * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理
         */
        for (int i = 1; i <= 10; i++) {
            try {

                /**
                 * TopicTest要发送的队列
                 * TagA标签,可以达到再次过滤,消费端可以只消费TagA的消息类似于这样
                 * key可以在控制台根据key查询消息
                 * body消息体内容
                 */
                Message msg = new Message("TopicTest",// topic
                        "TagA",// tag
                        "key1" + i,// key
                        ("订单一号" + i).getBytes());// body

                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        //这里的arg就是orderId传进来的
                        Integer id = (Integer) arg;
                        //取模决定放在哪个数据库
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 1);//订单号为1
                System.out.println(sendResult);


            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }
        for (int i = 1; i <= 10; i++) {
            try {
                int orderId = i;
                Message msg = new Message("TopicTest",// topic
                        "TagA",// tag
                        "key2" + i,// key
                        ("订单二号" + i).getBytes());// body
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        //这里的arg就是orderId传进来的
                        Integer id = (Integer) arg;
                        //取模决定放在哪个数据库
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 2);//订单号为2
                System.out.println(sendResult);


            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }
        for (int i = 1; i <= 10; i++) {
            try {
                int orderId = i;
                Message msg = new Message("TopicTest",// topic
                        "TagA",// tag
                        "key3" + i,// key
                        ("订单三号" + i).getBytes());// body
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        //这里的arg就是orderId传进来的
                        Integer id = (Integer) arg;
                        //取模决定放在哪个数据库
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 3);//订单号为3
                System.out.println(sendResult);

            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }


        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        producer.shutdown();
    }

}