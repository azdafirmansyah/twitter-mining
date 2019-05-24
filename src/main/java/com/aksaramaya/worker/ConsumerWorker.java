package com.aksaramaya.worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWorker implements Runnable {

//    private String topic;
    private CountDownLatch latch;
    private KafkaConsumer<String, String> kafkaConsumer;

    private Logger logger = LoggerFactory.getLogger(ConsumerWorker.class.getName());

    public ConsumerWorker(String bootstrapServer, String groupId, String topic,
                          String offset, CountDownLatch latch){
        this.latch = latch;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    public void run() {
        try{
            while (true){
                ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> consumer:consumerRecord) {
                    logger.info("Key        : " + consumer.key());
                    logger.info("Value      : " + consumer.value());
                    logger.info("Partition  : " + consumer.partition());
                    logger.info("Offset     : " + consumer.offset());
                }
            }
        }catch (Exception e){
            logger.error("Receive Shutdown Signal!!!");
        }finally {
            // Close Consumer
            kafkaConsumer.close();

            // Tell the main code, done with consumer
            latch.countDown();
        }
    }

    public void shutdown(){
        // The wakeup method to interrupt consumer.poll()
        // It will thrown exception
        kafkaConsumer.wakeup();
    }
}
