package consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerMultiThread {
    static final String brokerList = "localhost:9092";
    static final String topic = "topic-demo";
    static final String groupId = "group.demo1";
    static final AtomicBoolean isRunning = new AtomicBoolean(true);
    static Properties properties = new Properties();
    private static SynchronousQueue<ConsumerRecord<String, String>> queue = new SynchronousQueue<>();
    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(
            10,
            20,
            10,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    static {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.dmeo");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10 * 60 * 1000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        new Thread(() -> {
            while (isRunning.get()) {
                consumer.subscribe(Collections.singletonList(topic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    try {
                        queue.put(record);
                        consumer.commitSync();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }).start();
        initPipelineThread();
    }

    public static void processRecordItem(ConsumerRecord<String, String> record) {
        System.out.println(Thread.currentThread().getName() + record.toString());
    }
    static void initPipelineThread() {
        while (isRunning.get()) {
            try {
                final ConsumerRecord<String, String> record = queue.poll(5, TimeUnit.MINUTES);
                if (null != record)
                    executor.submit(() -> {
                        processRecordItem(record);
                    });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
