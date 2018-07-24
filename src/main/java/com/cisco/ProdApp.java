package com.cisco;

public class ProdApp {
    public static void main( String[] args ) {
        Prod producer = new Prod();
        producer.playback("Result-Kafka", 3);
    }
}
