package esercizio1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger log= LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String topic="august";
        // Crea properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "FourthGroup");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Crea consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        // Sottoscrivo il/i topic(s)
        consumer.subscribe(Collections.singleton(topic));
        // fossero più di uno
        //consumer.subscribe(Arrays.asList(topicList));

        // Estraggo
        while (true){
           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

           for (ConsumerRecord r : records){
               log.info(r.key() + " "+ r.value());
               log.info("INFO: partition " + r.partition() + " offset "+ r.offset());
           }
        }


    }
}
