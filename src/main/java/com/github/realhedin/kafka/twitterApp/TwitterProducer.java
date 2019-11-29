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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
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
        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

        //add shutdown hook to proper stop application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("closing application");
            logger.info("stopping client");
            client.stop();
            logger.info("closing producer");
            kafkaProducer.close();
            logger.info("application stopped");
        }));

        //send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info("Message: "+msg);
                    //send tweets into kafka topic
                    kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, e) -> {
                        if (e != null) {
                            logger.error("Error while producing", e);
                        }
                    });
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
     * create a Kafka producer to write messages into Kafka.
     * @return kafka producer.
     */
    private KafkaProducer<String,String> createKafkaProducer() {
        String bootstrapServers = "localhost:9092";

        //create via Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //make Producer safe
        //It includes: acks = all, retries = max, inflight_reg_per_connection = 5
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //create producer
        return new KafkaProducer<>(properties);

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
