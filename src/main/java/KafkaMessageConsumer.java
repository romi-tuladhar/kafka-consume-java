import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaMessageConsumer {

    public static void main(String[] args) {
        // Consumer group ID and Kafka server address
        String groupId = "my-consumer-group";
        String bootstrapServers = "localhost:9092"; // Use your Kafka broker address
        String topic = "consumer.new"; // Your Kafka topic name

        // Create consumer configurations
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // "earliest" to start from the beginning if no offset is committed
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(java.util.Collections.singletonList(topic));

        // Poll messages from the topic
        try {
            while (true) {
                // Adjust the polling duration as needed
                var records = consumer.poll(java.time.Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.println("Consumed message: " + record.value());
                });
            }
        } finally {
            consumer.close();
        }
    }
}
