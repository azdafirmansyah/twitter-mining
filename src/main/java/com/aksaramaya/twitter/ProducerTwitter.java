package com.aksaramaya.twitter;

import com.aksaramaya.handler.ProducerHandler;
import com.aksaramaya.helper.ProducerHelper;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProducerTwitter {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerTwitter.class.getName());

        // Twitter Auth
        String key = "gzE4wMysjGw4r6zbuTLhJoDQg";
        String secretKey = "qycBwkA6YNfhoZUvWg3lG7tDuiweuk3IybmUsfdnA8EUjWBkKE";
        String token = "306267198-2uCjvk6edHBKkwMCgxDD11tRqJq0w6OO2iX4jvT7";
        String secretToken = "aW9XjV0uTFGD8P4NaRKlrECj9jCFv7exUBgfjoA69ZaJZ";

        // Init Kafka Producer
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "twitter-topic";
        String valueMsg = "msg from twitter";

//        List<String> keywordTwitter = Lists.newArrayList("LIVBAR","ucl","championsleague",
//                "roadtomadrid","YNWA","lfc","liverpool","thisisanfield");

//        List<String> keywordTwitter = Lists.newArrayList("MenangTerhormat","NgabuburitReceh","jokowi",
//        "prabowo","kpu","situng","TiketPewatTerlaluMahal","UlamaTolakOBOR");

        List<String> keywordTwitter = Lists.newArrayList("testaksaramayatweet");


        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create Twitter Client
        final Client client = ProducerHandler.createTwitterClient(msgQueue, keywordTwitter,
                key, secretKey, token, secretToken);
        client.connect();

        // Create Producer
        KafkaProducer<String, String> kafkaProducer = ProducerHelper.createKafkaProducer(bootstrapServer);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting down application !!!");
            logger.info("Shutting down client from twitter");

            client.stop();

            logger.info("Closing Producer");
            logger.info("Done Close Producer!!!");
        }));

        //Loop to send tweet to kafka
        while (!client.isDone()){
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (Exception e){
                logger.error("Error while run ",e);
                client.stop();
            }

            if (msg != null) {
                logger.info("Incoming msg : " +msg);

                kafkaProducer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happen : ",e);
                        }
                    }
                });
            }
        }
        logger.info("End of Application !!!");


//        for (int i=0; i<10; i++) {
//            // Create Producer Record
//            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,valueMsg +" "+ Integer.toString(i));
//
//            // Send Data Asynchronous
//            kafkaProducer.send(producerRecord, new Callback() {
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e == null) {
//                        logger.info("Received new metadata : \n" +
//                                "Topic      : " + recordMetadata.topic() + "\n" +
//                                "Partition  : " + recordMetadata.partition() + "\n" +
//                                "Offset     : " + recordMetadata.offset() + "\n" +
//                                "Timestamp  : " + recordMetadata.timestamp() + "\n" +
//                                "Message    : " + recordMetadata.toString());
//                    }else{
//                        logger.error("Error while producing ", e);
//                    }
//                }
//            });
//        }
//
//        // Flush Data
//        kafkaProducer.flush();
//
//        // Close Producer
//        kafkaProducer.close();
    }
}
