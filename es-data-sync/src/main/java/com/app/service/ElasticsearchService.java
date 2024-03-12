package com.app.service;

import com.alibaba.fastjson.JSONObject;
import com.app.model.ConstantModel;
import com.app.model.ElasticsearchDataWrapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @Author miaoyoulin
 * @ClassName ElasticsearchService
 * @Description es索引业务查询类
 * @Date 2022/12/27 15:57
 * @Version 1.0
 **/
@Slf4j
public class ElasticsearchService {

    /**
     * es7.x 索引文档ID的最大字节数
     */
    private static final Integer INDEX_DOC_ID_BYTES_MAX = 512;

    /**
     * 判断索引是否存在
     * @param client es客户端
     * @param indexName 索引名称
     * @return true - 存在， false - 不存在
     * @throws IOException
     */
    public boolean isExists(RestHighLevelClient client,String indexName) throws IOException {
        //1、创建获取索引的请求
        GetIndexRequest request = new GetIndexRequest(indexName);
        //2、获取索引是否存在的结果
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 获取索引映射信息
     * @param client es客户端
     * @param indexName
     * @return 返回mapping映射的json字符串
     */
    public String getIndexMapping(RestHighLevelClient client,String indexName) throws IOException {
        //构建请求并设置索引名称
        GetMappingsRequest mappingsRequest = new GetMappingsRequest();
        mappingsRequest.indices(indexName);
        GetMappingsResponse response = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT);
        MappingMetadata mappingMetadata = response.mappings().get(indexName);

        return JSONObject.toJSONString(mappingMetadata.getSourceAsMap());
    }

    /**
     * 创建索引
     * @param client es客户端
     * @param indexName 索引名称
     * @param mapping 索引映射信息
     */
    public CreateIndexResponse createIndex(RestHighLevelClient client,String indexName, String mapping) throws IOException {
        //1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.mapping(mapping, XContentType.JSON);
        //2、执行创建索引的请求
        return client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 统计总数
     * @param client es客户端
     * @param indexName 索引名称
     * @return
     * @throws IOException
     */
    public Long countToTal(RestHighLevelClient client, String indexName) throws IOException {
        CountRequest countRequest = new CountRequest(indexName);
        //统计所有
        CountResponse count  = client.count(countRequest, RequestOptions.DEFAULT);
        if(count == null){
            return 0L;
        }
        return count.getCount();
    }


    /**
     * searchAfter查询
     * @param client es客户端
     * @param indexName 索引名称
     * @param objects 排序数组
     * @param limit 每次查询的条数
     * @return 返回es数据包装类，es中数据包装于其中
     */
    public ElasticsearchDataWrapper searchAfter(RestHighLevelClient client, String indexName, Object[] objects, Integer limit) throws IOException {
        //防止索引名称是否为自定义的输出索引与输入索引拼接而成,类似形式为 inputIndex -> outputIndex,下标为0的是输入索引，下标为1的是输出索引
        String[] split = indexName.split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        String inputIndexName = split[0];
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //每次查询的条数
        sourceBuilder.size(limit);
        //默认按照id倒叙排序
        sourceBuilder.sort("_id", SortOrder.DESC);
        if(objects[0].toString().equals("start") == false){
            sourceBuilder.searchAfter(objects);
        }
        SearchRequest searchRequest = new SearchRequest(inputIndexName);
        searchRequest.source(sourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        ElasticsearchDataWrapper dataWrapper = new ElasticsearchDataWrapper(indexName,hits.length);
        if(hits.length > 0){
            //记录下最后一组数据的排序数组
            dataWrapper.setSortValues(hits[hits.length - 1].getSortValues());
        }
        for (SearchHit hit : hits) {
            ElasticsearchDataWrapper.DataEntity dataEntity = dataWrapper.new DataEntity(hit.getId(), hit.getSourceAsString());
            dataWrapper.getEntityList().add(dataEntity);
        }
        return dataWrapper;
    }

    /**
     * scroll查询方式的初始查询
     * @param client
     * @param indexName
     * @param limit
     * @return
     */
    public ElasticsearchDataWrapper scrollBefore(RestHighLevelClient client, String indexName, Integer limit) throws IOException {
        //防止索引名称是否为自定义的输出索引与输入索引拼接而成,类似形式为 inputIndex -> outputIndex,下标为0的是输入索引，下标为1的是输出索引
        String[] split = indexName.split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        String inputIndexName = split[0];
        // 游标查询的过期时间会在每次做查询的时候刷新，所以这个时间只需要足够处理当前批的结果就可以了,这里使用1分钟
        Scroll scroll = new Scroll(TimeValue.timeValueSeconds(60L));
        SearchRequest searchRequest = new SearchRequest(inputIndexName);
        //构建查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(limit);
        //关键字 _doc 是最有效的排序顺序
        //如非必要，不建议添加排序字段，因为查询很慢
        //searchSourceBuilder.sort("_doc",SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        //设置深度分页
        searchRequest.scroll(scroll);
        //查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        ElasticsearchDataWrapper dataWrapper = new ElasticsearchDataWrapper(indexName,hits.length);
        if(hits.length > 0){
            //记录下最后一组数据的排序数组
            dataWrapper.setScrollId(searchResponse.getScrollId());
        }
        for (SearchHit hit : hits) {
            ElasticsearchDataWrapper.DataEntity dataEntity = dataWrapper.new DataEntity(hit.getId(), hit.getSourceAsString());
            dataWrapper.getEntityList().add(dataEntity);
        }
        return dataWrapper;
    }

    /**
     * 深度分页查询
     * @param scrollId 深度分页查询所需的ID
     * @return
     */
    public ElasticsearchDataWrapper scrollSearch(RestHighLevelClient client,String indexName, String scrollId) throws IOException {
        // 游标查询的过期时间会在每次做查询的时候刷新，所以这个时间只需要足够处理当前批的结果就可以了,每批次scrollId最长保留1分钟
        Scroll scroll = new Scroll(TimeValue.timeValueSeconds(60L));
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(scroll);
        //查询
        SearchResponse response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        ElasticsearchDataWrapper dataWrapper = new ElasticsearchDataWrapper(indexName,hits.length);
        if(hits.length > 0){
            //记录下最后一组数据的排序数组
            dataWrapper.setScrollId(response.getScrollId());
        }
        for (SearchHit hit : hits) {
            ElasticsearchDataWrapper.DataEntity dataEntity = dataWrapper.new DataEntity(hit.getId(), hit.getSourceAsString());
            dataWrapper.getEntityList().add(dataEntity);
        }
        return dataWrapper;
    }

    /**
     * 关闭滚动查询
     * @param scrollIds
     * @return
     * @throws IOException
     */
    public boolean closeScroll(RestHighLevelClient client,List<String> scrollIds) throws IOException {
        // 清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 也可以选择setScrollIds()将多个scrollId一起使用
        clearScrollRequest.setScrollIds(scrollIds);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        if (clearScrollResponse != null) {
            return clearScrollResponse.isSucceeded();
        }
        return false;
    }





    /**
     * 批量保存
     * @param client 客户端
     * @param dataWrapper 数据
     * @throws IOException
     */
    public void bulkSave(RestHighLevelClient client, ElasticsearchDataWrapper dataWrapper) throws IOException {
        List<ElasticsearchDataWrapper.DataEntity> entityList = dataWrapper.getEntityList();
        //防止索引名称是否为自定义的输出索引与输入索引拼接而成,类似形式为 inputIndex -> outputIndex,下标为0的是输入索引，下标为1的是输出索引
        String[] split = dataWrapper.getIndexName().split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        String outputIndexName = split.length > 1 ? split[1] : split[0];
        //1、创建批请求
        BulkRequest bulkRequest = new BulkRequest();
        for (ElasticsearchDataWrapper.DataEntity dataEntity : entityList) {
            //判断ID字段的字节数，es7.x以上,_id字段字节数不能超过521
            int docIdByteLength = dataEntity.getDocId().getBytes(StandardCharsets.UTF_8).length;
            if(docIdByteLength > INDEX_DOC_ID_BYTES_MAX){
                log.warn("索引名称为[{}],文档id为[{}]的数据,id字段字节数为[{}],超过最大值[{}],在本次同步中忽略!",outputIndexName,dataEntity.getDocId(),docIdByteLength,INDEX_DOC_ID_BYTES_MAX);
                continue;
            }
            IndexRequest indexRequest = new IndexRequest(outputIndexName);
            indexRequest.id(dataEntity.getDocId());
            indexRequest.source(dataEntity.getJsonObjectStr(), XContentType.JSON);
            //不覆盖已存在数据
            indexRequest.create(true);
            bulkRequest.add(indexRequest);
        }
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
