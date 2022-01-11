package com.mzyupc;

import com.alibaba.fastjson.JSON;
import io.redisearch.AggregationResult;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.SortedField;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * JRediSearch 示例
 *
 * @author mzyupc@163.com
 * @date 2022/1/11 2:59 下午
 */
public class JRediSearchExample {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static void insertBooks() {
        Map<String, String> book1 = new HashMap<>();
        book1.put("title", "book 1 title");
        book1.put("subtitle", "book 1 subtitle");
        book1.put("content", "The live Bitcoin price today is $42,178.34 USD with a 24-hour trading volume of $34,050,281,021 USD. We update our BTC to USD price in real-time. Bitcoin is up 0.82% in the last 24 hours. The current CoinMarketCap ranking is #1, with a live market cap of $798,259,325,351 USD. It has a circulating supply of 18,925,812 BTC coins and a max. supply of 21,000,000 BTC coins.");
        book1.put("publishAt", "1578708586000");

        Map<String, String> book2 = new HashMap<>();
        book2.put("title", "book 2 title");
        book2.put("subtitle", "book 2 subtitle");
        book2.put("content", "The live Ethereum price today is $3,112.16 USD with a 24-hour trading volume of $20,256,294,451 USD. We update our ETH to USD price in real-time. Ethereum is down 1.17% in the last 24 hours. The current CoinMarketCap ranking is #2, with a live market cap of $370,698,966,033 USD. It has a circulating supply of 119,112,892 ETH coins and the max. supply is not available.");
        book2.put("publishAt", "1610330986000");

        Map<String, String> book3 = new HashMap<>();
        book3.put("title", "book 3 title");
        book3.put("subtitle", "book 3 subtitle");
        book3.put("content", "The live Tether price today is $1.00 USD with a 24-hour trading volume of $72,227,675,169 USD. We update our USDT to USD price in real-time. Tether is down 0.01% in the last 24 hours. The current CoinMarketCap ranking is #3, with a live market cap of $78,282,212,811 USD. It has a circulating supply of 78,279,163,467 USDT coins and the max. supply is not available.");
        book3.put("publishAt", "1547172586000");

        try (Jedis jedis = new Jedis(HOST, PORT)) {
            jedis.hmset("book:1", book1);
            jedis.hmset("book:2", book2);
            jedis.hmset("book:3", book3);
        }
    }

    /**
     * 创建索引
     */
    private static void createIndex() {
        try (Client client = new Client("idx-java", HOST, PORT)) {

            IndexDefinition indexDefinition = new IndexDefinition().setPrefixes("book:");

            Schema schema = new Schema()
                    .addTextField("content", 1.0)
                    .addTextField("title", 5)
                    .addNumericField("publishAt");
            client.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));
        }
    }

    /**
     * 查询
     */
    private static void search() {
        try (Client client = new Client("idx-java", HOST, PORT)) {

            Query query = new Query("Tether").limit(0, 5);

            SearchResult searchResult = client.search(query);
            System.out.println(JSON.toJSONString(searchResult, true));
        }
    }

    /**
     * 聚合查询
     */
    private static void aggregate() {
        try (Client client = new Client("idx-java", HOST, PORT)) {

            // 聚合查询
            AggregationBuilder aggregationBuilder = new AggregationBuilder("circulating")
                    // 聚合中使用的索引字段必须是 SORTABLE, 否则必须load
                    .load("title", "content", "publishAt")
                    .apply("@content", "post")
                    .apply("@publishAt/1000", "publishAt")
                    .sortBy(SortedField.asc("@title"));
            AggregationResult aggregationResult = client.aggregate(aggregationBuilder);
            System.out.println(JSON.toJSONString(aggregationResult, true));
        }
    }

    private static void dropIndex() {
        try (Client client = new Client("idx-java", HOST, PORT)) {
            // 会删除关联的document, 相当于 ft.dropindex DD
            client.dropIndex();
        }
    }

    public static void main(String[] args) {
//        insertBooks();
//        createIndex();
        search();
//        aggregate();
    }
}
