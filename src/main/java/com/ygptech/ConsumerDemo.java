package com.ygptech;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {
        ConsumerDemo consumerDemo = new ConsumerDemo();
        consumerDemo.consumer();
    }

    /**
     * This is kafka consumer which consumes messages from first_topic
     */
    private void consumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-second-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("first_topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                // Just for debugging
                System.out.println("Added debugging to consumer");
                logger.info("Key : "+record.key()+",  value : "+record.value()+",  offset : "+record.offset()+",  partition : "+record.partition());
            }
        }
    }
}
