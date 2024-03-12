package com.app;

import com.app.core.DataSyncProcessor;
import com.app.client.impl.ElasticsearchClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @Author miaoyoulin
 * @ClassName EsDump
 * @Description es数据同步主类
 * @Date 2022/12/30 17:36
 * @Version 1.0
 **/
@Slf4j
public class EsDump {

    /**
     * 数据迁移步骤
     * 1、初始化输入和输出客户端，将不存在的索引先创建，将需要同步的索引的数据
     * 2、输入和输出各10条线程，将数据读取出来并包装后放入同步队列中，输出端线程从队列中读取数据存入对应索引中
     * 3、同步过程中将各索引同步的数据量记录下来
     *
     * *************************************************
     *
     * 本地测试账号:
     * host：www.baidu.com
     * port: 80
     * username: your_username
     * password: your_password
     * testIndexArray = {"index1","index2","index3","index4"}
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //数据源es配置
        String host1 = args[0];
        String username1 = args[1];
        String password1 = args[2];
        Integer port1 = Integer.parseInt(args[3]);

        //目标es配置
        String host2 = args[4];
        String username2 = args[5];
        String password2 = args[6];
        Integer port2 = Integer.parseInt(args[7]);

        //双端阻塞队列长度
        Integer dequeSize = Integer.parseInt(args[8]);
        //单次传输数量
        Integer singleTransferSize = Integer.parseInt(args[9]);

        //要迁移的索引,数组形式,以英文逗号[,]分隔
        //如果 isCustomOutputIndex = true，即自定义输出索引，那么自定义索引数组长度必须是偶数，数组值组成形式为 [inputIndex1,outputIndex1, inputIndex2,outputIndex12, inputIndex3,outputIndex3],输入索引和输出索引一一对应
        String[] indexArray = args[10].split(",");

        //双端队列监听超时时间，默认为10,建议设置成30,单位固定为s
        Long dequeListenerTimeout = Long.parseLong(args[11]);

        // true - 是，false - 否, 默认值为false
        //如果 isCustomOutputIndex = true，即自定义输出索引，那么自定义索引数组长度必须是偶数，数组值组成形式为 [inputIndex1,outputIndex1, inputIndex2,outputIndex12, inputIndex3,outputIndex3],输入索引和输出索引一一对应
        Boolean isCustomOutputIndex = Boolean.parseBoolean(args[12]);

        long startTime = System.currentTimeMillis();
        //构建es输入客户端
        RestHighLevelClient inputClient = new ElasticsearchClientBuilder(username1, password1, host1, port1).buildClient();
        //构建es输出客户端
        RestHighLevelClient outputClient = new ElasticsearchClientBuilder(username2, password2, host2, port2).buildClient();
        //构建数据迁移任务处理器
        DataSyncProcessor dataSyncProcessor = DataSyncProcessor.buildDataSyncProcessor(inputClient, outputClient, indexArray, dequeSize, singleTransferSize, isCustomOutputIndex,dequeListenerTimeout);
        try {
            //初始化
            dataSyncProcessor.init();
            //开始
            dataSyncProcessor.start();
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            if(dataSyncProcessor != null){
                dataSyncProcessor.destroy();
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("数据迁移任务结束,耗时[{}]ms, [{}]s", (endTime - startTime), (endTime - startTime) / 1000);
    }
}
