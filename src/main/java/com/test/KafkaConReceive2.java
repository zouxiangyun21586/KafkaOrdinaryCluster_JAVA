package com.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 消费者
 * 
 * @author liucong
 *
 * @date 2017年8月19日
 */
public class KafkaConReceive2 extends Thread {
	private String topic;

	public KafkaConReceive2(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到: " + message);
		}
	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
//		properties.put("zookeeper.connect", "192.168.1.244:2181,192.168.1.245:2181,192.168.1.246:2181");// 声明zk
		properties.put("zookeeper.connect", "192.168.1.138:2181,192.168.1.175:2181,192.168.1.180:2181");// 声明zk
		properties.put("group.id", "0");// 必须要使用别的组名称，
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}

	public static void main(String[] args) {
		new KafkaConReceive2("zouxiangyu-zxy").start();// 使用kafka集群中创建好的主题 test

	}
}
