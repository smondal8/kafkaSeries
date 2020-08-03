import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    static Logger LOG = LoggerFactory.getLogger(ProducerWithCallback.class);
    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String,String> record = new ProducerRecord("communicate","Let's go!!");
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    LOG.info("Time stamp : "+recordMetadata.timestamp());
                    LOG.info("Offset : "+recordMetadata.offset());
                    LOG.info("Topic : "+recordMetadata.topic());
                }
                else{
                    LOG.error("Some issue happend.");
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
