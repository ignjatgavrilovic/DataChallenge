package com.datachallenge;

public class DataChallengeConsumer {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("3 arguments must be submitted, topic to consume, topic to produce to and bootstrap server");
            return;
        }

        String consumeTopic = args[0];
        String produceTopic = args[1];
        String bootstrapServer = args[2];

        new CustomConsumer(consumeTopic, produceTopic, bootstrapServer).run();
    }
}
