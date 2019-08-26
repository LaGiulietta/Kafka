package esercizio1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallbackAndKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class);


        //Crea properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Crea producer
        final KafkaProducer<String,String> producer=new KafkaProducer(properties);

        for (int i=0;i<10;i++){
            String topic="third_topic";
            String value="Hello world "+i;
            String key= "id_" +i;

            log.info(key);

            //Send data
            ProducerRecord<String, String> record=new ProducerRecord<String, String>(topic, key, value);
            // Si fornisce una callback
            producer.send(record, new Callback( ) {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Codice da eseguire ogni volta che un invio ha successo o è sollevata un'eccezione
                    if (e == null) {
                        log.info("Received new metadata\nRecord: "+recordMetadata.topic()+
                                "\nPartition: "+recordMetadata.partition()+
                                "\nOffset: "+recordMetadata.offset());
                    } else {
                        log.info("Error while producing",e);

                    }
                }
            }).get();

        }
        producer.flush();
        producer.close();
    }
}
