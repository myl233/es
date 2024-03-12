package com.app.task;

import com.app.core.DataSyncProcessor;
import com.app.model.ConstantModel;
import com.app.model.ElasticsearchDataWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @Author miaoyoulin
 * @ClassName ScrollSearchTask
 * @Description scroll 方式读取
 * @Date 2023/1/11 22:12
 * @Version 1.0
 **/
@Slf4j
public class ScrollSearchTask implements Runnable{



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
     * 构造方法
     * @param config
     * @param indexName
     */
    public ScrollSearchTask(DataSyncProcessor.DataSyncConfig config, String indexName){
        this.config = config;
        this.indexName = indexName;
    }



    @Override
    public void run() {
        String[] split = indexName.split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        String inputIndexName = split[0];
        Set<String> scrollIds = new HashSet<>(1000);
        try {
            long start = System.currentTimeMillis();
            ElasticsearchDataWrapper dataWrapper = this.config.getElasticsearchService().scrollBefore(this.config.getInputClient(), this.indexName, this.config.getSingleTransferSize());
            while (dataWrapper.getScrollId() !=null && dataWrapper.getEntityList().isEmpty() == false){
                try {
                    //存放进入队列中
                    this.config.getBlockingDeque().put(dataWrapper);
                    //记录查询出的数据量
                    this.readCount(inputIndexName, dataWrapper.getEntityList().size());
                    long end = System.currentTimeMillis();
                    log.info("索引同步任务 -> [{}],本批次读取的数据量 -> [{}], 耗时[{}ms], [{}s]",this.indexName,dataWrapper.getEntityList().size(), end - start, (end - start) / 1000);
                    //重新计时
                    start = System.currentTimeMillis();
                    //记录下旧的scrollId
                    scrollIds.add(dataWrapper.getScrollId());
                    dataWrapper = this.config.getElasticsearchService().scrollSearch(this.config.getInputClient(), this.indexName, dataWrapper.getScrollId());
                }catch (Exception e){
                    log.error("索引同步任务 -> [" + indexName + "], 读取异常." + e.getMessage(),e);
                    this.exceptionCount--;
                    if(exceptionCount <= 0){
                        break;
                    }
                }
            }
            //清空scrollId
            this.config.getElasticsearchService().closeScroll(this.config.getInputClient(),new ArrayList<>(scrollIds));
            log.info("任务[{}]清空的scrollId个数:[{}]",this.indexName,scrollIds.size());
        }catch (Exception e){
            log.error("索引同步任务 -> [" + indexName + "],第一次执行时,读取异常." + e.getMessage(),e);
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
