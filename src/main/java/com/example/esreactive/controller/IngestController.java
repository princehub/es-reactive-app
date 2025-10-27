package com.example.esreactive.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
// IndexResponse not used
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@RestController
public class IngestController {

    private final ElasticsearchClient es;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${data.file:classpath:sample_products.ndjson}")
    private Resource dataFile;

    public IngestController(ElasticsearchClient es) {
        this.es = es;
    }

    @PostMapping(path = "/ingest", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<java.util.Map<String, Object>>> ingest(@RequestParam(name = "index", defaultValue = "products") String index) {
        return Mono.fromCallable(() -> {
            List<BulkOperation> ops = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(dataFile.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    // parse to generic map
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> map = mapper.readValue(line, java.util.Map.class);
                    String id = map.getOrDefault("productId", java.util.UUID.randomUUID().toString()).toString();
                    BulkOperation op = new BulkOperation.Builder().index(b -> b.index(index).id(id).document(map)).build();
                    ops.add(op);
                }
            }

            co.elastic.clients.elasticsearch.core.BulkRequest bulkRequest = new BulkRequest.Builder().operations(ops).index(index).build();
            BulkResponse resp = es.bulk(bulkRequest);
            java.util.Map<String, Object> out = new java.util.HashMap<>();
            out.put("ingested", resp.items().size());
            out.put("errors", resp.errors());
            return ResponseEntity.ok(out);
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

}
