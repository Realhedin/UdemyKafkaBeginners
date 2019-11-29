package com.github.realhedin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //create properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String me_fourth_group = "me_fourth_group";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, me_fourth_group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create Consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe on list of topics
        String topic = "first_topic";
        consumer.subscribe(Collections.singletonList(topic));

        //poll the messages from topic
        while(true) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(300));

            for (ConsumerRecord<String, String> message : messages) {
                logger.info("Key: " + message.key() + ", Value: " + message.value());
                logger.info("Partition: " + message.partition() + ", Offset:" + message.offset());
            }

        }
    }
}
