package com.app.task;

import com.app.core.DataSyncProcessor;
import com.app.model.ConstantModel;
import com.app.model.DataTotalCount;
import com.app.model.ElasticsearchDataWrapper;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author miaoyoulin
 * @ClassName SearchAfterTask
 * @Description 数据读取task searchAfter 查询方式
 * @Date 2022/12/28 17:50
 * @Version 1.0
 **/
@Slf4j
public class SearchAfterTask implements Runnable {



    private DataSyncProcessor.DataSyncConfig config;

    /**
     * 索引名称
     */
    private String indexName;

    /**
     * 异常次数
     */
    private  Integer exceptionCount = 10;

    /**
     * 构造函数
     * @param config 数据同步相关配置
     * @param indexName 同步的索引名称
     */
    public SearchAfterTask(DataSyncProcessor.DataSyncConfig config, String indexName){

        this.config = config;
        this.indexName = indexName;
    }



    @Override
    public void run() {
        String[] split = indexName.split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        String inputIndexName = split[0];
        // 数据读取逻辑
        Object[] objects = {"start"};
        while (true){
            try {
                ElasticsearchDataWrapper dataWrapper = this.config.getElasticsearchService().searchAfter(this.config.getInputClient(), indexName, objects, this.config.getSingleTransferSize());
                if(dataWrapper.getEntityList().size() <= 0){
                    break;
                }
                //获取最后一组的排序规则
                objects = dataWrapper.getSortValues();
                //存放进入队列中
                this.config.getBlockingDeque().put(dataWrapper);
                //记录查询出的数据量
                this.readCount(inputIndexName, dataWrapper.getEntityList().size());
                log.info("索引同步任务 -> [{}],本批次读取的数据量 -> [{}]",indexName,dataWrapper.getEntityList().size());
            }catch (Exception e){
                log.error("索引同步任务 -> [" + indexName + "], 读取异常." + e.getMessage(),e);
                this.exceptionCount--;
                if(exceptionCount <= 0){
                    break;
                }
            }
        }
        log.info("索引同步任务 -> [{}],读取完成! 读取的数据量 -> [{}]",indexName,Integer.parseInt(this.config.getTotalCountArray()[this.config.getTotalCountArrayIndexMap().get(inputIndexName) + 1].toString()));
    }

    private void readCount(String indexName, Integer size){
        Integer index = this.config.getTotalCountArrayIndexMap().get(indexName);
        //计算步长,读取数据，步长固定为1
        //先记录已写出的数据
        Integer sum = Integer.parseInt(this.config.getTotalCountArray()[index + 1].toString());
        sum += size;
        this.config.getTotalCountArray()[index + 1] = sum;
    }
}
