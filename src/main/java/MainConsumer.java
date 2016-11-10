import org.apache.avro.generic.IndexedRecord;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.errors.SerializationException;
import java.util.*;
/**
 * Created by davidvegaperez on 10/11/16.
 */
public class MainConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "group1");
        props.put("schema.registry.url", "http://localhost:8082");

        String topic = "page_visits";
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));

        VerifiableProperties vProps = new VerifiableProperties(props);
        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
                topicCountMap, keyDecoder, valueDecoder);
        List<KafkaStream<Object, Object>> streams = consumerMap.get(topic);
        KafkaStream stream = consumerMap.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
            try {
                String key = (String) messageAndMetadata.key();
                IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
                System.out.println("HOLA");
            } catch(SerializationException e) {
                // may need to do something with it
            }
        }
    }
}
