package esercizio1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Leggere messaggi già letti specificando offset di partenza e numero messaggi da leggere
 */
public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger log= LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String topic="third_topic";
        // Crea properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Crea consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        // assign and seek --> Vogliamo ripetere dati o ricevere uno specifico messaggio
        // 1. Assign
        TopicPartition part= new TopicPartition(topic, 0);
        long offset=15L;
        consumer.assign(Arrays.asList(part));

        // 2. Seek
        consumer.seek(part, offset);

        int numOfMessToRead= 5;
        boolean keepOnReading=true;
        int numOfMessReaded=0;

        // Estraggo
        while (keepOnReading){
           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

           for (ConsumerRecord r : records){
               numOfMessReaded++;
               log.info(String.valueOf(numOfMessReaded));
               log.info(r.key() + " "+ r.value());
               log.info("INFO: partition " + r.partition() + " offset "+ r.offset());
               if (numOfMessReaded >= numOfMessToRead) {
                   keepOnReading=false;
                   break;
               }
           }
        }
        log.info("Exiting");


    }
}
