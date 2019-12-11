package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerFastStart {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "group.demo");


        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList("topic-demo"));
        while (true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record : records){
                System.out.println(record.value());
            }
        }
    }
}
