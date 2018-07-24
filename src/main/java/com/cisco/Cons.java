package com.cisco;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.*;

public class Cons {
    private Properties captureProperties;
    private KafkaConsumer<String, String> capturer;

    Cons() {
        captureProperties = new Properties();
        captureProperties.put("bootstrap.servers", "localhost:9092");
        captureProperties.put("group.id", "test");
        captureProperties.put("enable.auto.commit", "true");
        captureProperties.put("auto.commit.interval.ms", "1000");
        captureProperties.put("session.timeout.ms", "30000");
        captureProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        captureProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        capturer = new KafkaConsumer<String, String>(captureProperties);
    }

    public void capture(String topicName, final long duration){
        capturer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
        System.out.println("Subscribed to a topic " + topicName);
        long endTime = new Date().getTime() + duration * 1000;

        try {
            BufferedWriter bwr = new BufferedWriter(new FileWriter(new File("capture.txt")));
            while (true) {
                if (new Date().getTime() > endTime) {
                    break;
                }
                ConsumerRecords<String, String> records = capturer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    bwr.write(record.key() + " " + record.value());
                    bwr.newLine();
                }
            }
            bwr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
