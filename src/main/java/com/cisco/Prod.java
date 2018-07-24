package com.cisco;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class Prod {
    Properties props;
    Producer<String, String> producer;

    Prod() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        producer = new KafkaProducer<String, String>(props);
    }

    public void playback(String topicName, int numberOfReps) {
        for (int i = 0; i < numberOfReps; i++) {
            try {
                FileReader in = new FileReader("capture.txt");
                BufferedReader br = new BufferedReader(in);
                String line;
                while ((line = br.readLine()) != null) {
                    String parts[] = line.split("\\s");
                    ProducerRecord<String, String> record =
                            new ProducerRecord<String, String>(topicName, parts[0], parts[1]);
                    producer.send(record);
                }
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Message sent!");
        producer.close();
    }
}
