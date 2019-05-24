package com.aksaramaya.handler;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProducerHandler {



    public static Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> keywordList,
                                      String key, String secretKey, String token, String secretToken){

        Logger logger = LoggerFactory.getLogger(ProducerHandler.class.getName());
        logger.info("Create Twitter Client!!!");

//        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
//        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        //Setup auth host
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hostsEndPoint = new StatusesFilterEndpoint();
        hostsEndPoint.trackTerms(keywordList);

        // Hardcode twitter auth for temp : key, secretkey, token, secrettoken
        Authentication auth = new OAuth1(key, secretKey, token, secretToken);

        ClientBuilder builder = new ClientBuilder()
                .name("Aksaramaya-Cleint-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(hostsEndPoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue); //use this to process client event

        Client hostClient = builder.build();
        return hostClient;

    }
}
