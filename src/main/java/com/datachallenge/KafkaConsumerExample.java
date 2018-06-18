package com.datachallenge;

import com.datachallenge.model.UniqueUsers;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.*;

public class KafkaConsumerExample {

    private static final String TOPIC = "test";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    private static long initialTimestamp = 0;

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        return consumer;
    }

    static void runConsumer() throws Exception {

        final Consumer<Long, String> consumer = createConsumer();
        Map<String, Integer> uniqueUsersMap = new HashMap<>();
        Map<String, Integer> uniqueUsersMapTmp = new HashMap<>();
        final List<UniqueUsers> result = new ArrayList<>();
        final int giveUp = 25;   int noRecordsCount = 0;

        boolean fiveSecondRule = false;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                System.out.println("No records count: " + noRecordsCount);
                if (noRecordsCount > giveUp) break;
                else continue;
            }


            for (ConsumerRecord<Long, String> record : consumerRecords) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode obj = mapper.readTree(record.value());

                    long timestamp = obj.findValues("timestamp").get(0).asLong();
                    String user = obj.findValuesAsText("user_id").get(0);

                    // Logic
                    if (initialTimestamp == 0) {
                        initialTimestamp = timestamp / 60 * 60;
                    }

                    if (fiveSecondRule) {
                        if (timestamp < initialTimestamp) {
                            uniqueUsersMap.put(user, 1);
                        } else {
                            uniqueUsersMapTmp.put(user, 1);
                        }
                    } else {
                        uniqueUsersMap.put(user, 1);
                    }

                    if (timestamp - initialTimestamp >= 60) {
                        initialTimestamp = timestamp / 60 * 60;
                        fiveSecondRule = true;
                        uniqueUsersMapTmp = new HashMap<>();
                    }

                    if (fiveSecondRule && timestamp - initialTimestamp > 5) {
                        fiveSecondRule = false;
                        result.add(new UniqueUsers(initialTimestamp - 60, uniqueUsersMap.size()));
                        uniqueUsersMap = uniqueUsersMapTmp;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            consumer.commitAsync();
        }

        result.add(new UniqueUsers(initialTimestamp, uniqueUsersMap.size())); // TODO revisit what map to add (only one or combination)
        boolean writeToConsole = false;
        if (writeToConsole) {
            result.forEach(uniqueUser -> System.out.println(uniqueUser.toJson()));
        } else {
            KafkaProducerExample.runProducer(result);
        }

        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws Exception {
        runConsumer();
    }
}
