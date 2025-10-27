package com.example.esreactive.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.util.List;

@Configuration
public class ElasticsearchConfig {

//    @Value("${elasticsearch.hosts}")
//    private List<String> hosts; // not used directly; we'll use property string formats in YAML

    @Value("${elasticsearch.username}")
    private String username;

    @Value("${elasticsearch.password}")
    private String password;

    @Value("${elasticsearch.verify-certs:false}")
    private boolean verifyCerts;

    @Bean(destroyMethod = "close")
    public RestClient restClient() throws Exception {
        // Build an SSLContext that trusts all certs if verifyCerts is false (local dev convenience)
        SSLContext sslContext;
        if (!verifyCerts) {
            sslContext = SSLContextBuilder.create().loadTrustMaterial((TrustStrategy) (chain, authType) -> true).build();
        } else {
            sslContext = SSLContext.getDefault();
        }

    final CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(org.apache.http.HttpHost.create("https://localhost:9200"));
    // set SSL and creds via HttpClientConfigCallback (uses HttpComponents 4.x types provided by the ES REST client)
    builder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
        httpAsyncClientBuilder.setSSLContext(sslContext)
            .setDefaultCredentialsProvider(credsProvider)
    );

        return builder.build();
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(RestClient restClient) {
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}
