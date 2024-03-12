package com.app.client.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author miaoyoulin
 * @ClassName ElasticsearchClientBuilder
 * @Description es客户端初始化
 * @Date 2022/12/26 15:45
 * @Version 1.0
 **/
@Slf4j
public class ElasticsearchClientBuilder{


    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String password;

    /**
     * ip/域名
     */
    private String host;

    /**
     * 端口，默认80
     */
    private Integer port;

    /**
     * 协议，默认为http
     */
    private String scheme;

    /**
     * 最大总连接数
     */
    private static final Integer MAX_CONN_TOTAL = 500;

    /**
     * 每个路由值的最大连接数
     */
    private static final Integer MAX_CONN_PER_ROUTE = 300;


    /**
     * 构造方法
     * @param userName 用户名
     * @param password 密码
     * @param host IP或者域名
     * @param port 端口
     */
    public ElasticsearchClientBuilder(String userName, String password, String host, Integer port) {
        this.userName = userName;
        this.password = password;
        this.host = host;
        this.port = port;
        this.scheme = port == 443 ? "https" : "http";
    }


    /**
     * 构建客户端
     * @return 返回一个 RestHighLevelClient
     * @throws IOException
     */
    public RestHighLevelClient buildClient() throws IOException {

        RestHighLevelClient restHighLevelClient = null;
        try {
            RestClientBuilder restClientBuilder = getRestClientBuilder();
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
            return restHighLevelClient;
        }catch (Exception e){
            log.error("elasticsearch client build failed! errorMsg:" + e.getMessage(),e);
            if(restHighLevelClient != null){
                restHighLevelClient.close();
            }
            throw new RuntimeException("elasticsearch client build failed!",e);
        }
    }

    /**
     * 构建一个restClient的builder
     * @return RestClientBuilder
     */
    private RestClientBuilder getRestClientBuilder() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        return RestClient.builder(
                new HttpHost(host, port, scheme)
        ).setRequestConfigCallback(builder -> {
            builder.setConnectTimeout(60000);
            builder.setSocketTimeout(60000);
            builder.setConnectionRequestTimeout(60000);
            return builder;
        }).setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
            httpAsyncClientBuilder.setMaxConnTotal(MAX_CONN_TOTAL);
            httpAsyncClientBuilder.setMaxConnPerRoute(MAX_CONN_PER_ROUTE);
            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        });
    }

    private static JSONObject getFromElasticsearch(RestClient client, String endpoint) throws Exception {
        Request request = new Request("GET", endpoint);
        Response response = client.performRequest(request);
        return (JSONObject) JSONObject.parse(IOUtils.toString(response.getEntity().getContent()));
    }

    private static void setClusterNodes(RestClient client) throws Exception {
        final JSONObject nodes = getFromElasticsearch(client, "/_nodes").getJSONObject("nodes");
        final List<Node> nodeList = nodes.values().stream().map(node ->
                        new Node(HttpHost.create("http://" + ((JSONObject) node).getJSONObject("http").getString("publish_address"))))
                .collect(Collectors.toList());
        client.setNodes(nodeList);
    }
}
