package org.apache.rocketmq.test.demo;
 
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
 
import java.util.List;
 
public class OrderConsumer {
 
    public static void main(String[] args) throws InterruptedException,
            MQClientException {
 
        /**
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例
         * 注意：ConsumerGroupName需要由应用来保证唯一
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName");
        consumer.setNamesrvAddr("172.20.1.186:9876");
        consumer.setInstanceName("Consumer1");
        consumer.setMessageModel(MessageModel.CLUSTERING);
 
        //消费线程最小数量
        //consumer.setConsumeThreadMin(30);
        //消费线程最大数量
        //consumer.setConsumeThreadMax(100);
 
 
        /**
         * 订阅指定topic下tags分别等于TagA或TagC或TagD, 这里没有订阅TagB的消息,所以不会消费标签为TagB的消息，*代表不过滤 接受一切
         */
        //consumer.subscribe("TopicTest", "TagA || TagC || TagD");
        consumer.subscribe("TopicTest", "*");
 
        /**
         * 如果是顺序消息,这边的监听就要使用MessageListenerOrderly监听
         * 并且,返回结果也要使用ConsumeOrderlyStatus
         */
        //MessageListenerConcurrently这种消费者是无法做到顺序消费的
        //MessageListenerOrderly能够保证顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                //设置自动提交,如果不设置自动提交就算返回SUCCESS,消费者关闭重启 还是会重复消费的
                context.setAutoCommit(true);
                try {
                    for (MessageExt msg:msgs) {
                        System.out.println(" 消费者1 ==> 当前线程:"+Thread.currentThread().getName()+" ,quenuID: "+msg.getQueueId()+ " ,content: " + new String(msg.getBody()));
                    }
 
                } catch (Exception e) {
                    e.printStackTrace();
                    //如果出现异常,消费失败，挂起消费队列一会会，稍后继续消费
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
 
                //消费成功
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
 
        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可
         */
        consumer.start();
 
        System.out.println("C1 Started.");
    }
}