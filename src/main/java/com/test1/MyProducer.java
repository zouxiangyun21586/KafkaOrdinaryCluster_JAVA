package com.test1;
import java.util.Properties; 
   
import kafka.javaapi.producer.Producer; 
import kafka.producer.KeyedMessage; 
import kafka.producer.ProducerConfig; 
  
/**
 * 生产者 (发布订阅)
 * @author zxy
 *
 */
public class MyProducer {   
     
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list","192.168.1.175:9092,192.168.1.138:9092,192.168.1.180:9092");
        props.setProperty("serializer.class","kafka.serializer.StringEncoder");
        /**
         * 0: 这意味着生产者producer不等待来自broker同步完成的确认继续发送下一条(批)消息。
         * 	    此选项提供最低的延迟但最弱的耐久性保证（当服务器发生故障时某些数据会丢失，
         *	    如leader已挂，但producer并不知情，发出去的信息broker就收不到）
         *   --- 不等待broker的确认信息,最小延迟
         * 1: 这意味着producer在leader已成功收到的数据并得到确认后发送下一条message。
         *    此选项提供了更好的耐久性为客户等待服务器确认请求成功（被写入死亡leader但尚未复制将失去了唯一的消息）。
         *   --- leard 已经接收了数据的确认信息,Replica异步拉取消息,比较折中 
         * -1: 这意味着producer在follower副本确认接收到数据后才算一次发送完成。 
		 *     此选项提供最好的耐久性，我们保证没有信息将丢失，只要至少一个同步副本保持存活。
		 *   --- ISR列表中所有Replica都返回确认消息
		 * 三种机制，性能依次递减 (producer吞吐量降低)，数据健壮性则依次递增
         */
        props.put("request.required.acks","-1");
        ProducerConfig config = new ProducerConfig(props);
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        //生成消息
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("zxyOO","邹想云-----zxy");
        try {
            int i = 0;
            while(i < 10){
                //发送消息
                producer.send(data);
                i++;
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}