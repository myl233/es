package com.app.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author miaoyoulin
 * @ClassName ElasticsearchDataWrapper
 * @Description es数据包装类
 * @Date 2022/12/27 17:04
 * @Version 1.0
 **/
@Data
public class ElasticsearchDataWrapper {

    /**
     * 索引名称
     */
    private String indexName;


    /**
     * searchAfter 深度分页时使用
     */
    private Object[] sortValues;

    /**
     * scroll 深度分页时使用
     */
    private String scrollId;

    /**
     * 数据实体集合
     */
    private List<DataEntity> entityList;

    /**
     * 构造方法
     * @param indexName 索引名称
     * @param size 集合大小
     */
    public ElasticsearchDataWrapper(String indexName, Integer size){
        this.indexName = indexName;
        this.entityList = new ArrayList<>(size);
    }

    /**
     * 数据实体
     */
    @Data
    @AllArgsConstructor
    public class DataEntity {
        /**
         * 文档ID
         */
        private String docId;

        /**
         * 数据，以json字符串形式存放
         */
        private String jsonObjectStr;
    }
}
