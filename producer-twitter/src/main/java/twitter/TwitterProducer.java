package twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger log= LoggerFactory.getLogger(TwitterProducer.class.getName());    // These secrets should be read from a config file
    String consumerKey="9jA1TsPzWiLeZO3hNd5WjJ3om";
    String consumerSecret="mk3ULBmY8Tpe91zOXjwIlptf2DuvkqHQmKmmRNTsJka7zpw8hx";
    String token="428969972-go2hBAKVrgvhTfospE5sz70aTJuzcupLNaCSoY25";
    String secret="eHODSEsFVsJms9L29dyzUJh9teDM1kZBo4otTgL3Ffbz2";
    List<String> terms = Lists.newArrayList("soccer", "usa", "music");

    BlockingDeque<String> msgQueue = new LinkedBlockingDeque<String>(1000);
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        log.info("Setup");

        // create twitter client
        Client client= createTwitterClient(msgQueue);
        client.connect(); //  Attempts to estabilish a connection
        // create kafka producer
        KafkaProducer<String,String> producer=createKafkaProducer();


        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->
        {
            log.info("stopping application");
            log.info("shutting down client from twitter");
            client.stop();
            log.info("closing producer");
            producer.close();
            log.info("..done");
        }));


        // send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace( );
                client.stop();
            }
            if (msg != null){
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_topics", null, msg), new Callback( ) {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //Codice da eseguire ogni volta che un invio ha successo o è sollevata un'eccezione
                        if (e != null) {
                            log.info("Error while producing",e);
                        } else {


                        }
                    }
                });

            }
        }
        log.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //Crea properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        // High throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //Crea producer
        return new KafkaProducer(properties);
    }

    public Client createTwitterClient(BlockingDeque<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Create client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(this.msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
