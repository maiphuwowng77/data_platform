package com.viettel.vdt.kafka;

import org.apache.kafka.clients.producer.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final String TOPIC_NAME = "vdt2024";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String dataPath = "../data/log_action.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
            String line;
            int recordCount = 0;

            while ((line = reader.readLine()) != null) {
                // Process the CSV line and create a ProducerRecord
                String[] fields = line.split(",");
                String key = fields[0]; // Control partitions of records
                // Create a ProducerRecord with topic name, key and value
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, line);

                // Send the record and handle the result with callback
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Record id " + record.key() + " sent successfully - topic: " + metadata.topic() +
                                    ", partition: " + metadata.partition() +
                                    ", offset: " + metadata.offset());

                        } else {
                            System.err.println("Error while sending record " + record.key() + ": " + exception.getMessage());
                        }
                    }
                });
                recordCount++;

            }
            System.out.println(recordCount + " records sent to Kafka successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getKey (String [] fields) {
        if (Integer.parseInt(fields[0]) % 3 == 0)
            return "selected";
        else return "other";
    }

}
