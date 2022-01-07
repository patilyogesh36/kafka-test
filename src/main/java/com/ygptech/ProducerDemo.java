package com.ygptech;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Properties;

public class ProducerDemo {

    Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.producer();
    }

    /**
     * This is the kafka-producer which produces messages to first_topic
     */
    public void producer() {
        //properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //send data
        for (int i=1; i<=10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "id_"+i,i+" message from intellij IDEA");
            // Just for debugging
            System.out.println("Added debugging to Producer");
            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata." +
                            "\nTopic:" + metadata.topic() +
                            "\nPartition:" + metadata.partition() +
                            "\nOffset:" + metadata.offset() +
                            "\nTimestamp:" + new Timestamp(System.currentTimeMillis()));
                } else {
                    logger.error("Error while producing.", e);
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
