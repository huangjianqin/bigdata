//package org.kin.bigdata.rabbitmq;
//
//import com.rabbitmq.client.*;
//
//import java.io.IOException;
//import java.nio.charset.Charset;
//import java.util.concurrent.TimeoutException;
//
///**
// * Created by 健勤 on 2017/7/30.
// */
//public class RabbitMQDemo {
//    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
//        channel.queueDeclare("test", false, false, false, null);
//        channel.basicPublish("", "test", null, "test".getBytes(Charset.forName("utf-8")));
//        channel.close();
//        connection.close();
//
//        ConnectionFactory factory1 = new ConnectionFactory();
//        factory1.setHost("localhost");
//        Connection connection1 = factory1.newConnection();
//        Channel channel1 = connection1.createChannel();
//        channel1.queueDeclare("test", false, false, false, null);
//
//        Consumer consumer = new DefaultConsumer(channel1) {
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                System.out.println(new String(body, "utf-8"));
//            }
//        };
//        channel1.basicConsume("test", true, consumer);
//        Thread.sleep(2000);
//        channel1.close();
//        connection1.close();
//    }
//}
