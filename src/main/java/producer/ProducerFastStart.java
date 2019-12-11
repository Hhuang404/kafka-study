package producer;


import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerFastStart {
    public static KafkaProducer<String, String> initConfig() {
        Properties properties = new Properties();
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
//        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializatizer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("bootstrap.servers", "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        return producer;
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = initConfig();
//        Company company = Company.builder().name("Apache.kafka").address("new street").build();
        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "company");
        try {
//            producer.send(record).get();
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println(metadata.topic() + "-" +
                            metadata.partition() + ":" + metadata.offset());
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
