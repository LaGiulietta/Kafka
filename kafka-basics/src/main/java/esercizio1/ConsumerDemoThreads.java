package esercizio1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args) {

        new ConsumerDemoThreads( ).run( );

    }


    private void run() {
        Logger log = LoggerFactory.getLogger(ConsumerDemoThreads.class.getName( ));

        String topic = "third_topic";
        String groupID = "ThirdGroup";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // creating runnable consumer
        log.info("Creating consumer...  ");
        Runnable c = new ConsumerRunnable(latch, groupID, topic);

        // start the thread

            Thread myThread = new Thread(c);
            myThread.start( );


        // add shutdown hook
        Runtime.getRuntime( ).addShutdownHook(new Thread(
                () -> {
                    log.info("Cought shutdown hook");
                    ((ConsumerRunnable) c).shutdown( );
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace( );
                    }
                    log.info("application has exited");

                }));
        try {
            latch.await( );
        } catch (InterruptedException e) {
            log.error("application interrupted", e);
        }
        log.info("application is closing");

    }


    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName( ));

        public ConsumerRunnable(CountDownLatch l, String groupId, String topic) {
            this.latch = l;

            // Crea properties
            Properties properties = new Properties( );
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName( ));
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName( ));

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Crea consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // Sottoscrivo il/i topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                // Estraggo
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord r : records){
                        log.info(r.key() + " "+ r.value());
                        log.info("INFO: partition " + r.partition() + " offset "+ r.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Shutdown signal");
            } finally {
                consumer.close( );
                latch.countDown( );
            }
        }

        public void shutdown() {
            consumer.wakeup( ); //interrupt consumer.poll() -- solleva WakeUpException
        }

    }
}
