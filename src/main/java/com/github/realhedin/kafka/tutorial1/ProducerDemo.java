package com.github.realhedin.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {


        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String bootstrapServers = "localhost:9092";

        //create via Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        //create ProducerRecord
        ProducerRecord<String, String> producerRecord;

        for (int i = 0; i < 10; i++) {
            //fill data in producerRecord
            producerRecord = new ProducerRecord<>("first_topic", "hello kafka from API " + i);
            //send data
            kafkaProducer.send(producerRecord, ((recordMetadata, exception) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (exception == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", exception);
                }
            }));
        }

        //to force wait until sent
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
