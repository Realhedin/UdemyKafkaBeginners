package com.github.realhedin.kafka.twitterApp;

import com.google.common.collect.Lists;
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
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    //TODO fill this values with your own parameters from Twitter Development application
    private String consumerKey = "consumerKey";
    private String consumerSecret = "consumerSecret";
    private String token = "token";
    private String secret = "secret";

    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    private BlockingQueue<String> msgQueue;

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    private void run() {
        logger.info("Application started.");
        //create twitter client
        msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClient();
        client.connect();

        //create kafka producer

        //sent tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info("Message: "+msg);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                client.stop();
                logger.info("end of application");
            }
        }
    }

    /**
     * fully prepare Twitter client.
     * @return ready to use client.
     */
    private Client createTwitterClient() {
        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(this.msgQueue));

        // Attempts to establish a connection.
        return builder.build();
    }
}
