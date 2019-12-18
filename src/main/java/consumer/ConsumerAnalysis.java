
package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ALL")
@Slf4j
public class ConsumerAnalysis {
    static final String brokerList = "localhost:9092";
    static final String topic = "topic-demo";
    static final String groupId = "group.demo1";
    static final AtomicBoolean isRunning = new AtomicBoolean(true);
    static Properties properties = new Properties();

    static {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.dmeo");
    }

    /**
     * 同步提交
     * 每条消息都细粒度的同步消费位移，性能极地
     */
    @Test
    public void commitSyncTest1() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                System.out.println(record.toString());
                long offset = record.offset();
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
            });
        }
    }

    /**
     * 同步提交
     * 按分区粒度同步消费位移
     */
    @Test
    public void commitSyncTest2() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                partitionRecords.forEach(record -> {
                    // do some logical proccessing.
                });
                // 定位最后一条消息的 offset
                long lastConsumeOfset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumeOfset + 1)));
            });
        }
    }

    /**
     * 异步消费
     */
    @Test
    public void commitAsyncTest1() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(tp));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.partitions().forEach(partition -> {
                System.out.println(partition.toString());
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) System.out.println(record.toString());
            });
            // 同步提交
            consumer.commitSync();

            // 异步提交
            consumer.commitAsync((offsets, exception) -> {
                if (exception == null) {
                    System.out.println(offsets);
                } else
                    System.out.println("fail to commit offsets " + offsets + exception);
            });
        }

    }

    /**
     * 指定消费
     */
    @Test
    public void seekTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        assignment.forEach(tp -> consumer.seek(tp, 10));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(e -> System.out.println(e.toString()));
        }
    }

    /**
     * 使用
     * endOffSet() 获取末尾的 offset 位置
     */
    @Test
    public void seekEndOffSetTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        assignment.forEach(tp -> consumer.seek(tp, offsets.get(tp)));
    }

    /**
     * 自定时间消费
     * 通过 offsetsForTimes()
     */
    @Test
    public void seekUseTimestamp() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        Map<TopicPartition, Long> timestampToSearch = new HashMap();
        assignment.forEach(tp -> timestampToSearch.put(tp, System.currentTimeMillis() - 1 * 24 * 3600 * 1000));
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        assignment.forEach(tp -> {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        });
    }

    /**
     * 在均衡用法
     */
    @Test
    public void ConsumerRebalanceListenerTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            });
            consumer.commitAsync(currentOffsets, null);
        }
    }

    @Test
    public void firstMultiConsumerThreadTest() throws InterruptedException {
        CountDownLatch count = new CountDownLatch(4);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(
//                4,
//                4,
//                1,
//                TimeUnit.MINUTES,
//                new ArrayBlockingQueue(100)
//        );
//        executor.execute(() -> {
//            consumer.subscribe(Arrays.asList(topic));
//            try {
//                while (isRunning.get()) {
//                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//                    records.forEach(record -> {
//                        System.out.println(Thread.currentThread().getName() + record.toString());
//                    });
//                }
//            } catch (Exception e) {
//            } finally {
//                consumer.close();
//                count.countDown();
//            }
//        });

        for (int i = 4; i > 0; i--) {
            new kafkaConsumerThread(properties, topic).start();
        }

        count.await();
    }

    public static class kafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        public kafkaConsumerThread(Properties properties, String topic) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (isRunning.get()){
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10));
                    records.forEach(record -> {
                        System.out.println(Thread.currentThread().getName() + record.toString());
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

}
