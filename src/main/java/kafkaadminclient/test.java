package kafkaadminclient;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class test {

    static String brokerList = "localhost:9092";
    static String topic = "topic-admin";
    static Properties properties = new Properties();

    static {
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
    }

    @Test
    public void create() {
        AdminClient client = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }

    @Test
    public void describeTopic() {
        AdminClient client = AdminClient.create(properties);
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> stringTopicDescriptionMap =
                    describeTopicsResult.all().get();
            System.out.println(stringTopicDescriptionMap);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void describeTopicConfig() {
        AdminClient client = AdminClient.create(properties);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        try {
            Config config = result.all().get().get(resource);
            System.out.println(config);
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
