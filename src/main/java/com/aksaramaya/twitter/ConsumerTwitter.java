package com.aksaramaya.twitter;

import com.aksaramaya.worker.ConsumerWorker;
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

public class ConsumerTwitter {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerTwitter.class.getName());

        //Init Consumer
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "group-high-school";
        String topic = "twitter-topic";
        String offset = "earliest";

        CountDownLatch latch = new CountDownLatch(1);

        final Runnable worker = new ConsumerWorker(bootstrapServer, groupId, topic, offset, latch);

        Thread consumerThread = new Thread(worker);
        consumerThread.run();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutddown Hook");
            ((ConsumerWorker) worker).shutdown();

            try{
                latch.await();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                logger.info("Application has exited!");
            }
        }

        ));

//        //Consumer Configs
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,autoReset);
//
//        //Create Kafka Consumer
//        KafkaConsumer<String, String> kafkaConsumer= new KafkaConsumer<String, String>(properties);
//
//        //Subscribe to topic
//        kafkaConsumer.subscribe(Arrays.asList(topic));
//
//        //Poll for incoming data
//        while(true){
//            ConsumerRecords<String, String> records =
//                    kafkaConsumer.poll(100); // new in Kafka 2.0.0
//
//            for (ConsumerRecord<String, String> record : records){
//                logger.info("Key: " + record.key() + ", Value: " + record.value());
//                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
//            }
//        }
    }
}
