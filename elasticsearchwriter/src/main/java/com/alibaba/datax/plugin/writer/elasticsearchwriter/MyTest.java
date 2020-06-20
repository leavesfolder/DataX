package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MyTest {
    public static void main(String[] args) {
        ESClient esClient = new ESClient();


        String endpoint="http://10.208.251.234:9146";
        boolean multiThread = true;
        int readTimeout = 600000;
        boolean compression = true;
        boolean discovery = false;
        String user = "iclws_r";
        String passwd = "iclws_r@360.cn";
        String index="ecs_event";
        String type="";


        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
                .Builder(endpoint)
                .setPreemptiveAuth(new HttpHost(endpoint))
                .multiThreaded(multiThread)
                .connTimeout(30000)
                .readTimeout(readTimeout)
                .maxTotalConnection(200)
                .requestCompressionEnabled(compression)
                .discoveryEnabled(discovery)
                .discoveryFrequency(5l, TimeUnit.MINUTES);

        if (!("".equals(user) || "".equals(passwd))) {
            httpClientConfig.defaultCredentials(user, passwd);
        }

        factory.setHttpClientConfig(httpClientConfig.build());

        JestClient jestClient = factory.getObject();

        String json="{'query': {'range': {'timestamp': {'gte': 1582905600000L, 'lte': 1582991999000L}}}}";
//        String json = "{\n" + "    \"query\" : {\n" + "        \"match\" : {\n"
//                + "            \"content\" : \"测试内容\"\n" + "        }\n"
//                + "    }\n" + "}";
        Search search = new Search.Builder(json).addIndex(index).addType(type).build();
        try {
            SearchResult result = jestClient.execute(search);
//            List<SearchResult.Hit<JsonObject, Void>> hits =
            List<SearchResult.Hit<JSONObject, Void>> hits = result.getHits(JSONObject.class);

            for(SearchResult.Hit<JSONObject, Void> hit: hits){
                JSONObject line = hit.source;
                System.out.println(line.get("eventCode"));
                System.out.println(line.get("oprContent"));
                break;
            }

            System.out.println(result.getJsonString());
        } catch(IOException e) {
            e.printStackTrace();
        }


    }
}
