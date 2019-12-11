package Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {
    private volatile Integer success = 0;
    private volatile Integer failure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix 1 - " + record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),
                record.timestamp(),record.key(),modifiedValue,
                record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null){
            success++;
        }else
            failure++;
    }

    @Override
    public void close() {
        System.out.println("成功 "+success);
        System.out.println("失败 "+failure);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
