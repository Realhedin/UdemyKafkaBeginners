package com.github.realhedin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        //create properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create Consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        String topic = "first_topic";

        //assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(topicPartition));

        //seek
        long offset = 15L;
        consumer.seek(topicPartition, offset);

        //poll the messages from topic
        int numOfMessages = 5;
        int counter = 0;
        boolean condition = true;
        while(condition) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(300));

            for (ConsumerRecord<String, String> message : messages) {
                logger.info("Key: " + message.key() + ", Value: " + message.value());
                logger.info("Partition: " + message.partition() + ", Offset:" + message.offset());
                counter++;
                if (counter >= numOfMessages) {
                    condition = false;
                    break;
                }
            }

        }
    }
}
