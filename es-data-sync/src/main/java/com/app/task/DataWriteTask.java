package com.app.task;

import com.app.core.DataSyncProcessor;
import com.app.model.ConstantModel;
import com.app.model.ElasticsearchDataWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * @Author miaoyoulin
 * @ClassName DataWriteTask
 * @Description 数据写出task
 * @Date 2022/12/28 17:51
 * @Version 1.0
 **/
@Slf4j
public class DataWriteTask implements Runnable{


    private DataSyncProcessor.DataSyncConfig config;

    /**
     * es数据包装类
     */
    private ElasticsearchDataWrapper dataWrapper;

    /**
     * 输出索引名称
     */
    private String outputIndexName;

    /**
     * 重试次数
     */
    private Integer retriesNum = 0;


    /**
     * 构造函数
     * @param config es数据同步相关配置
     * @param dataWrapper 数据包装类
     */
    public DataWriteTask(DataSyncProcessor.DataSyncConfig config, ElasticsearchDataWrapper dataWrapper) {
        this.config = config;
        this.dataWrapper = dataWrapper;
        String[] split = dataWrapper.getIndexName().split(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS);
        this.outputIndexName = split.length > 1 ? split[1] : split[0];
    }

    @Override
    public void run() {

        while (true){
            try {
                long start = System.currentTimeMillis();
                this.config.getElasticsearchService().bulkSave(this.config.getOutputClient(),dataWrapper);
                //写出数据统计
                this.writeCount(outputIndexName,dataWrapper.getEntityList().size());
                long end = System.currentTimeMillis();
                log.info("索引同步任务 -> [{}], 本批次写出的数据量 -> [{}],重试次数[{}]次, 耗时[{}ms],[{}s]",dataWrapper.getIndexName(),dataWrapper.getEntityList().size(),this.retriesNum, end - start, (end - start) / 1000);
                //写出成功,退出循环
                break;
            }catch (Exception e){
                //暂停10秒
                try {
                    TimeUnit.SECONDS.sleep(10);
                }catch (InterruptedException interruptedException){
                    log.error("索引数据写出任务暂停异常,msg:" + interruptedException.getMessage(),e);
                }
                this.retriesNum++;
                log.error("索引同步任务 -> [" + dataWrapper.getIndexName() + "],准备第[" + this.retriesNum +"]次重试.write-Exception-ErrorMsg:" + e.getMessage(),e);
                if(this.retriesNum > 10){
                    break;
                }
            }
        }
    }

    /**
     * 写出数据统计
     * @param indexName 索引名称
     * @param size 数据量
     */
    private void writeCount(String indexName, Integer size){
        Integer index = this.config.getTotalCountArrayIndexMap().get(indexName);
        //计算步长,自定义输出索引，index步长为1，非自定义输出索引步长为2
        Integer step = this.config.getIsCustomOutputIndex() ? 1 : 2;
        //先记录已写出的数据
        Integer sum = Integer.parseInt(this.config.getTotalCountArray()[index + step].toString());
        sum += size;
        this.config.getTotalCountArray()[index + step] = sum;
    }
}
