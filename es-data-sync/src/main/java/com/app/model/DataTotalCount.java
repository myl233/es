package com.app.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author miaoyoulin
 * @ClassName DataTotalCount
 * @Description 数据总数统计，用于打印日志统计使用
 * @Date 2022/12/29 17:04
 * @Version 1.0
 **/
@Slf4j
public class DataTotalCount {

    /**
     * 读取的数据总数,默认值为0
     */
    private Integer readTotalCount = 0;

    /**
     * 写出的数据总数，默认值为0
     */
    private Integer writeTotalCount = 0;


    /**
     * 读取数据累加
     * @param size 读取的数据
     */
    public void readTotalIncr(Integer size){
        this.readTotalCount += size;
    }

    /**
     * 写出数据累加
     * @param size
     */
    public void writeTotalCountIncr(Integer size){
        this.writeTotalCount += size;
    }

    public Integer getReadTotalCount() {
        return readTotalCount;
    }

    public Integer getWriteTotalCount() {
        return writeTotalCount;
    }
}
