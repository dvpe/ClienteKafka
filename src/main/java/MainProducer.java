import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * Created by davidvegaperez on 10/11/16.
 */
public class MainProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8082");

        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                "\"name\": \"page_visitttttt\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"site\", \"type\": \"string\"}," +
                "{\"name\": \"ip\", \"type\": \"string\"}," +
                "{\"name\": \"ip3\", \"type\": \"string\"}," +
                "{\"name\": \"ip2\", \"type\": \"string\"}" +
                "]}";

        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < 5; nEvents++) {
            long runtime = new Date().getTime();
            String site = "www.example.com";
            String ip = "192.168.2." + rnd.nextInt(255);

            GenericRecord page_visit = new GenericData.Record(schema);
            page_visit.put("time", runtime);
            page_visit.put("site", site);
            page_visit.put("ip", ip);
            page_visit.put("ip2", ip);

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>(
                    "page_visits", ip, page_visit);
            producer.send(data);
        }

        producer.close();
    }
}
