package com.example.esreactive.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.MultiMatchQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class SearchController {

    private final ElasticsearchClient es;
    private final ObjectMapper mapper = new ObjectMapper();

    public static class PinnedItem {
        public Object productId;
        public int pinPosition;
    }

    public static class SearchRequestBody {
        public String searchTerm;
        public List<PinnedItem> pinned = List.of();
        public int size = 20;
        public int pageNumber = 1;
    }

    public SearchController(ElasticsearchClient es) {
        this.es = es;
    }

    @PostMapping(path = "/search", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<java.util.Map<String, Object>>> search(@RequestBody SearchRequestBody body) {
        return Mono.fromCallable(() -> {
            String q = body.searchTerm == null ? "" : body.searchTerm;

            // normalize pinned ids to string ids (Pxxxx)
            List<String> pinnedIds = body.pinned.stream().map(p -> normalizePid(p.productId)).collect(Collectors.toList());

            // fetch pinned documents in a single mget call
            Map<String, java.util.Map<String, Object>> pinnedDocsMap = new HashMap<>();
            if (!pinnedIds.isEmpty()) {
                try {
                    // Use an ids query to fetch pinned docs in a single search request
                    Query idsQuery = Query.of(qb -> qb.ids(idb -> idb.values(pinnedIds)));
                    co.elastic.clients.elasticsearch.core.SearchRequest pinnedReq = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                            .index("products")
                            .query(idsQuery)
                            .size(pinnedIds.size())
                            .build();

                    SearchResponse<java.util.Map> presp = es.search(pinnedReq, java.util.Map.class);
                    if (presp.hits() != null) {
                        for (Hit<java.util.Map> hit : presp.hits().hits()) {
                            String id = hit.id();
                            java.util.Map<String, Object> src = mapper.convertValue(hit.source(), new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>(){});
                            pinnedDocsMap.put(id, src);
                        }
                    }
                } catch (Exception ignored) {
                    // ignore missing pins or search failure
                }
            }

            // calculate global range for requested page
            int size = Math.max(1, body.size);
            int page = Math.max(1, body.pageNumber);
            int globalStart = (page - 1) * size + 1; // 1-based
            int globalEnd = page * size; // inclusive

            // count how many pinned before page and collect pinned items inside the page
            List<PinnedItem> pinnedInPage = new ArrayList<>();
            int pinnedBefore = 0;
            for (PinnedItem p : body.pinned) {
                int pos = p.pinPosition;
                if (pos < globalStart) pinnedBefore++;
                else if (pos >= globalStart && pos <= globalEnd) pinnedInPage.add(p);
            }

            // (internal) how many non-pinned docs we need up to page end is globalEnd - pinnedBefore

            // perform ES search excluding pinned ids
            Query multi = MultiMatchQuery.of(m -> m.query(q).fields("title^3", "description", "category^2", "brand^2"))._toQuery();

            // Build a bool query that excludes pinned ids
            Query boolQuery = Query.of(qb -> qb.bool(b -> {
                b.must(multi);
                if (!pinnedIds.isEmpty()) {
                    b.mustNot(Query.of(mn -> mn.ids(idb -> idb.values(pinnedIds))));
                }
                return b;
            }));

            // number of non-pinned docs to fetch for this page
            int neededNonPinnedForPage = Math.max(0, size - pinnedInPage.size());

            List<java.util.Map<String, Object>> nonPinnedHits = new ArrayList<>();
            if (neededNonPinnedForPage > 0) {
                int fromNonPinned = Math.max(0, (globalStart - 1) - pinnedBefore);

                co.elastic.clients.elasticsearch.core.SearchRequest sreq = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                        .index("products")
                        .query(boolQuery)
                        .from(fromNonPinned)
                        .size(neededNonPinnedForPage)
                        .build();

                SearchResponse<java.util.Map> resp = es.search(sreq, java.util.Map.class);
                if (resp.hits() != null) {
                    for (Hit<java.util.Map> h : resp.hits().hits()) {
                        java.util.Map<String, Object> src = mapper.convertValue(h.source(), new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>(){});
                        nonPinnedHits.add(src);
                    }
                }
            }

            // assemble final page by inserting pinned docs at absolute positions and filling with non-pinned hits
            List<Object> pageResults = new ArrayList<>();
            Iterator<java.util.Map<String, Object>> nonPinnedIter = nonPinnedHits.iterator();

            Map<Integer, java.util.Map<String, Object>> pinMap = new HashMap<>();
            for (PinnedItem p : pinnedInPage) {
                String pid = normalizePid(p.productId);
                var doc = pinnedDocsMap.get(pid);
                if (doc != null) pinMap.put(p.pinPosition, doc);
            }

            for (int pos = globalStart; pos <= globalEnd; pos++) {
                if (pinMap.containsKey(pos)) {
                    pageResults.add(pinMap.get(pos));
                } else {
                    if (nonPinnedIter.hasNext()) pageResults.add(nonPinnedIter.next());
                }
            }

            java.util.Map<String, Object> out = new java.util.HashMap<>();
            out.put("pageNumber", page);
            out.put("size", size);
            out.put("results", pageResults);
            out.put("count", pageResults.size());
            return ResponseEntity.ok(out);
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    private String normalizePid(Object pid) {
        if (pid == null) return "";
        if (pid instanceof Number) {
            int v = ((Number) pid).intValue();
            return String.format("P%04d", v);
        }
        String s = pid.toString();
        if (s.matches("^\\d+$")) {
            return String.format("P%04d", Integer.parseInt(s));
        }
        return s;
    }
}
