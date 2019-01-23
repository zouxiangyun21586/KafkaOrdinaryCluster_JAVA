package com.test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;



/**
 * 生产者
 * 
 * @author liucong
 *
 * @date 2017年8月19日
 */
public class KafkaConProducre extends Thread {
	private String topic;

	public KafkaConProducre(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		Producer<Integer, String> producer = createProducer();
		for (int i = 1; i < 15; i++) {
			producer.send(new KeyedMessage<Integer, String>(topic, "message:" + i));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer<Integer, String> createProducer() {
		Properties properties = new Properties();
//		properties.put("zookeeper.connect", "192.168.1.244:2181,192.168.1.245:2181,192.168.1.246:2181");// 声明zk
		properties.put("zookeeper.connect", "192.168.1.138:2181,192.168.1.175:2181,192.168.1.180:2181");// 声明zk
		properties.put("serializer.class", StringEncoder.class.getName());
//		properties.put("metadata.broker.list", "192.168.1.244:9092,192.168.1.245:9092,192.168.1.246:9092");
		properties.put("metadata.broker.list", "192.168.1.138:9092,192.168.1.175:9092,192.168.1.180:9092");//
		// broker
		return new Producer<Integer, String>(new ProducerConfig(properties));
	}

	public static void main(String[] args) {
		new KafkaConProducre("zouxiangyu-zxy").start();// 使用kafka集群中创建好的主题 test
	}
}
