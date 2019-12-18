package producer;



import Interceptor.ProducerInterceptorPrefix;
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
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        return  new KafkaProducer(properties);
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = initConfig();
//        Company company = Company.builder().name("Apache.kafka").address("new street").build();
        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "company");
        try {
//            producer.send(record).get();
//            producer.send(record, (metadata, exception) -> {
//                if (exception != null) {
//                    exception.printStackTrace();
//                } else {
//                    System.out.println(metadata.topic() + "-" +
//                            metadata.partition() + ":" + metadata.offset());
//                }
//            });
            for (int i=100;i>0;i--)
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
