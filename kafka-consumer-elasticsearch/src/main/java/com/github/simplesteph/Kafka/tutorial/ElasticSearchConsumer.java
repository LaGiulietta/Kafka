package com.github.simplesteph.Kafka.tutorial;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {
    /**
     *
     * @return un'oggetto necessario per poter inserire dati in Elasticsearch
     */
    public static RestHighLevelClient createClient(){



        String hostName="kafka-course-5431193382.ap-southeast-2.bonsaisearch.net";
        String userName="egqurmcbam";
        String password="obn9kg3a78";

        CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
        credentialsProvider.setCredentials((AuthScope.ANY), new UsernamePasswordCredentials(userName, password));

        // Connessione all'host, attraverso HTTPS, con le credenziali settate precedentemente
        RestClientBuilder builder= RestClient.builder(new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback( ) {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client= new RestHighLevelClient(builder);
        return client;

    }

    public static void main(String[] args) throws IOException {
        Logger log= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client= createClient();

        String jsonString="{\"foo\":\"bar\"}";
        IndexRequest indexReq=new IndexRequest("twitter", "tweet" ).source(jsonString, XContentType.JSON);

        IndexResponse indexRes=client.index(indexReq, RequestOptions.DEFAULT);
        String idRes= indexRes.getId();
        log.info(idRes);

        client.close();

    }
}
