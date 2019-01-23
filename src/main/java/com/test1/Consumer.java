package com.test1;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * 消费者的线程执行器实现 -- 处理消息的类
 * @author Administrator
 *
 */
public class Consumer implements Runnable {
    
    private KafkaStream stream;
    private int threadNumber;
 
    public Consumer(KafkaStream a_stream, int a_threadNumber) {
        threadNumber = a_threadNumber;
        stream = a_stream;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext())
            System.out.println("Thread " + threadNumber + ": " + new String(it.next().message()));
        System.out.println("Shutting down Thread: " + threadNumber);
    }
}