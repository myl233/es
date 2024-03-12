package com.app.task;

import com.app.core.DataSyncProcessor;
import com.app.model.ElasticsearchDataWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

/**
 * @Author miaoyoulin
 * @ClassName DataQueueListener
 * @Description 数据同步队列监听器
 * @Date 2022/12/29 16:31
 * @Version 1.0
 **/
@Slf4j
public class DataQueueListener implements Callable<String> {

    private DataSyncProcessor.DataSyncConfig config;

    /**
     * 构造函数
     * @param config es数据同步相关配置
     */
    public DataQueueListener(DataSyncProcessor.DataSyncConfig config){
       this.config = config;
    }

    @Override
    public String call() throws Exception {
        while (true){
            try {
                //最长阻塞时间根据初始化时的配置来，阻塞时候过后队列中还没数据，说明数据迁移任务已全部完成
                ElasticsearchDataWrapper dataWrapper = this.config.getBlockingDeque().poll(this.config.getDequeListenerTimeout(), this.config.getDequeListenerTimeoutUnit());
                if(dataWrapper == null){
                    //数据读取完成
                    log.info("es data sync Finish!");
                    break;
                }
                //开启一个线程执行output任务
                DataWriteTask dataWriteTask = new DataWriteTask(config,dataWrapper);
                this.config.getExecutorService().execute(dataWriteTask);
            } catch (Exception e) {
                log.error("数据同步队列读取数据异常:" + e.getMessage(),e);
            }
        }
        return "OK";
    }
}
