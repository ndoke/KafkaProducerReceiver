package com.cisco;

public class ConsApp {
    public static void main(String[] args) {
        Cons consumer = new Cons();
        consumer.capture("Hello-Kafka", 10);
    }
}
