package com.app.core;

import com.app.service.ElasticsearchService;
import com.app.model.ConstantModel;
import com.app.model.ElasticsearchDataWrapper;
import com.app.task.DataQueueListener;
import com.app.task.ScrollSearchTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @Author miaoyoulin
 * @ClassName DataSyncProcessor
 * @Description 数据同步处理器
 * @Date 2022/12/28 17:40
 * @Version 1.0
 **/
@Slf4j
public class DataSyncProcessor {

    /**
     * 数据同步配置类
     */
    private DataSyncConfig config;


    /**
     * 构数据同步任务处理器
     * @param inputClient es数据输入客户端
     * @param outputClient es数据输出客户端
     * @param indexArray 索引数组
     * @param dequeSize 双端阻塞队列长度
     * @param singleTransferSize 单次传输数量
     * @param isCustomOutputIndex 是否自定义输出索引 true - 是，false - 否, 默认值为false
     * @param dequeListenerTimeout 双端队列监听阻塞时长，单位秒
     * @return
     */
    public static DataSyncProcessor buildDataSyncProcessor(RestHighLevelClient inputClient, RestHighLevelClient outputClient, String[] indexArray, Integer dequeSize, Integer singleTransferSize, Boolean isCustomOutputIndex, Long dequeListenerTimeout){
        DataSyncProcessor.DataSyncConfig config = new DataSyncProcessor.DataSyncConfig(inputClient,outputClient,indexArray,dequeSize,singleTransferSize,isCustomOutputIndex,dequeListenerTimeout);
        //构建任务处理对象
        return new DataSyncProcessor(config);
    }



    private DataSyncProcessor(DataSyncProcessor.DataSyncConfig config){
        this.config = config;
    }

    /**
     * 数据迁移初始化
     */
    public void init() throws Exception {
        log.info("es数据同步资源初始化开始......");
        //判断是否为自定义输出索引
        List<String> indexList = null;
        if(this.config.isCustomOutputIndex){
            log.info("使用自定义索引方式初始化...");
            indexList = this.customOutputIndex();
        }else {
            log.info("使用非自定义索引方式初始化...");
            indexList = this.notCustomOutputIndex();
        }

        if(indexList != null && indexList.size() != this.config.indexArray.length){
            //获取一个新索引数组
            String[] newIndexArray = indexList.toArray(new String[indexList.size()]);
            this.config.indexArray = newIndexArray;
        }
        //初始化索引线程池
        ExecutorService executorService = this.config.buildExecutorService(this.config.indexArray.length);
        this.config.executorService = executorService;
        log.info("es数据迁移资源初始化完成! 开始准备迁移数据,迁移的索引有[{}]个,任务名为{}",this.config.indexArray.length, Arrays.toString(this.config.indexArray));
    }


    /**
     * 开始同步任务
     */
    public void start() throws Exception {
        if(this.config.singleTransferSize <= 0){
            log.info("设置单次的传输数量为[{}],本次只同步索引结构! 数据迁移任务结束!",this.config.singleTransferSize);
            return;
        }
        log.info("es数据迁移任务开始执行! 任务数量:[{}]",this.config.indexArray.length);
        //1、启动队列监听
        DataQueueListener dataQueueListener = new DataQueueListener(this.config);
        Future<String> submit = this.config.executorService.submit(dataQueueListener);
        //2、启动读取数据任务
        for (String indexTask : this.config.indexArray) {
            ScrollSearchTask scrollSearchTask = new ScrollSearchTask(this.config, indexTask);
            ///不实用searchAfter读取数据,海量数据时较慢
            //SearchAfterTask searchAfterTask = new SearchAfterTask(this.config,indexTask);
            this.config.executorService.execute(scrollSearchTask);
        }
        String result = submit.get();
        //监听器线程任务完成后,关闭线程池,因为不会给线程池添加新任务了
        this.config.executorService.shutdown();
        //等待所有写出线程任务结束才关闭线程池,最长阻塞时间60分钟
        this.config.executorService.awaitTermination(60L,TimeUnit.MINUTES);
        for (int i = 0; i < this.config.totalCountArray.length;) {
            if(this.config.isCustomOutputIndex){
                //自定义输出索引
                log.info("从数据源索引:[{}]中读取的数据量:[{}] -> 往目标索引:[{}]中写入的数据量:[{}]",this.config.totalCountArray[i++],this.config.totalCountArray[i++],this.config.totalCountArray[i++],this.config.totalCountArray[i++]);
            }else {
                //非自定义输出索引
                log.info("索引名称为[{}]的同步任务,读取的数据量:[{}],写入的数据量:[{}]",this.config.totalCountArray[i++],this.config.totalCountArray[i++],this.config.totalCountArray[i++]);
            }

        }
        log.info("es数据迁移任务执行完成! result = [{}]",result);
    }

    /**
     * 结束后销毁相关实例的方法
     */
    public void destroy(){
        try {
            if(this.config.inputClient != null){
                this.config.inputClient.close();
            }
            if(this.config.outputClient != null){
                this.config.outputClient.close();
            }
        }catch (IOException e){
            log.error("执行销毁方法时异常,异常信息:" + e.getMessage(), e);
        }
    }

    /**
     * 非自定义输出索引的方式初始化
     * @return
     * @throws Exception
     */
    private List<String> notCustomOutputIndex() throws Exception{
        //校验索引是否已在es数据源中存在
        List<String> indexList = new ArrayList<>(this.config.indexArray.length);
        Long countTotal = 0L;
        //数据统计下标值
        Integer totalCountIndex = 0;
        for (String indexName : this.config.indexArray) {
            //检查读取数据源中是否存在索引
            boolean inputExists = this.config.elasticsearchService.isExists(this.config.inputClient, indexName);
            if(inputExists == false ){
                //不存在
                log.warn("名称为[{}]的索引,在读取数据源的es库中不存在! 本次同步忽略该索引.",indexName);
                continue;
            }
            //检查同步的目标数据源中是否存在索引
            boolean outputExists = this.config.elasticsearchService.isExists(this.config.outputClient, indexName);
            if(outputExists == false){
                //不存在，将读取数据源的es库中的索引复制到输出数据源的es库中
                String indexMapping = this.config.elasticsearchService.getIndexMapping(this.config.inputClient, indexName);
                this.config.elasticsearchService.createIndex(this.config.outputClient, indexName, indexMapping);
                log.info("名称为[{}]的索引,在输出数据源的es库中创建完成!",indexName);
            }
            //检查数据源索引中是否存在数据
            Long count = this.config.elasticsearchService.countToTal(this.config.inputClient, indexName);
            countTotal += count;
            if(count <= 0L){
                log.warn("数据源索引[{}]中不存在数据, 本次同步忽略该索引!",indexName);
                continue;
            }
            //记录统计数组中索引名称下标
            this.config.totalCountArrayIndexMap.put(indexName,totalCountIndex);
            //初始化数据统计,初始值默认为0
            this.config.totalCountArray[totalCountIndex++] = indexName;
            //读取数据量的初始值
            this.config.totalCountArray[totalCountIndex++] = 0;
            //写出数据量的初始值
            this.config.totalCountArray[totalCountIndex++] = 0;
            indexList.add(indexName);
        }
        log.info("总数据量为:[{}]",countTotal);
        return indexList;
    }

    /**
     * 自定义索引初始化
     * 自定义索引数组长度必须是偶数，数组形式为 [inputIndex1,outputIndex1, inputIndex2,outputIndex12, inputIndex3,outputIndex3],输入索引和输出索引一一对应
     */
    private List<String> customOutputIndex() throws Exception {
        if(this.config.indexArray.length % 2 != 0){
            //自定义索引数组长度必须是偶数
            throw new RuntimeException("The length of the custom index array must be an even number,but the current array length is [" + this.config.indexArray.length +"]");
        }
        Long countTotal = 0L;
        //数据统计下标值
        Integer totalCountIndex = 0;
        List<String> indexList = new ArrayList<>(this.config.indexArray.length / 2);
        for (int i = 0; i < this.config.indexArray.length;) {
            //校验数据源索引是否存在
            //获取数据源索引
            String sourceIndexName = this.config.indexArray[i++];
            boolean inputExists = this.config.elasticsearchService.isExists(this.config.inputClient, sourceIndexName);
            if(inputExists == false){
                //不存在，同时忽略数据源索引和目标索引，从下一组索引开始
                //获取目标索引
                String targetIndexName = this.config.indexArray[i++];
                log.warn("数据源索引[{}]不存在, 本次同步忽略该组索引! 忽略的索引组为 sourceIndexName -> [{}], targetIndexName -> [{}]",sourceIndexName,sourceIndexName,targetIndexName);
                continue;
            }

            //数据源索引存在,检查目标索引是否存在
            String targetIndexName = this.config.indexArray[i++];
            boolean outputExists = this.config.elasticsearchService.isExists(this.config.outputClient, targetIndexName);
            if(outputExists == false){
                //目标索引不存在，创建索引
                String indexMapping = this.config.elasticsearchService.getIndexMapping(this.config.inputClient, sourceIndexName);
                this.config.elasticsearchService.createIndex(this.config.outputClient, targetIndexName, indexMapping);
                log.info("目标索引名称为[{}]的索引,在输出数据源的es库中创建完成!",targetIndexName);
            }
            //检查数据源索引中是否存在数据
            Long count = this.config.elasticsearchService.countToTal(this.config.inputClient, sourceIndexName);
            countTotal += count;
            if(count <= 0L){
                log.warn("数据源索引[{}]中不存在数据, 本次同步忽略该组索引! 忽略的索引组为 sourceIndexName -> [{}], targetIndexName -> [{}]",sourceIndexName,sourceIndexName,targetIndexName);
                continue;
            }

            //记录统计数组中输入索引名称下标
            this.config.totalCountArrayIndexMap.put(sourceIndexName,totalCountIndex);
            //初始化数据统计,初始值默认为0
            this.config.totalCountArray[totalCountIndex++] = sourceIndexName;
            //读取数据量的初始值
            this.config.totalCountArray[totalCountIndex++] = 0;

            //记录统计数组中输出索引名称下标
            this.config.totalCountArrayIndexMap.put(targetIndexName,totalCountIndex);
            //初始化数据统计,初始值默认为0
            this.config.totalCountArray[totalCountIndex++] = targetIndexName;
            //写出数据量的初始值
            this.config.totalCountArray[totalCountIndex++] = 0;
            //拼接索引名称
            String indexSplit = new StringBuilder(sourceIndexName).append(ConstantModel.INDEX_NAME_SPLICE_SYMBOLS).append(targetIndexName).toString();
            indexList.add(indexSplit);
        }
        log.info("总数据量为:[{}]",countTotal);
        return indexList;
    }


    /**
     * 数据同步任务配置类
     */
    public static class DataSyncConfig {

        /**
         * 数据迁移，读取客户端
         */
        private RestHighLevelClient inputClient;

        /**
         * 数据迁移，输出客户端
         */
        private RestHighLevelClient outputClient;

        /**
         * es业务查询类
         */
        private ElasticsearchService elasticsearchService;

        /**
         * 线程池
         */
        private ExecutorService executorService;

        /**
         * 阻塞队列
         */
        private LinkedBlockingDeque<ElasticsearchDataWrapper> blockingDeque;

        /**
         * 需要迁移的索引
         * 如果 isCustomOutputIndex = true，即自定义输出索引，那么自定义索引数组长度必须是偶数，数组值组成形式为 [inputIndex1,outputIndex1, inputIndex2,outputIndex12, inputIndex3,outputIndex3],输入索引和输出索引一一对应
         */
        private String[] indexArray;

        /**
         * 数据统计使用
         */
        private Object[] totalCountArray;

        /**
         * 数据统计数组索引映射,记录每个索引名称在 totalCountArray 中的下标
         * 例如, index1 -> 0, index2 -> 1
         */
        private Map<String, Integer> totalCountArrayIndexMap;

        /**
         * 是否自定义输出索引
         * true - 是，false - 否 ，默认值为false
         */
        private Boolean isCustomOutputIndex = false;

        /**
         * 单次传输的数据量,默认值为500
         */
        private Integer singleTransferSize = 500;

        /**
         * 双端队列监听超时时间，默认为10
         */
        private Long dequeListenerTimeout = 10L;

        /**
         * 双端队列监听超时单位，默认为秒
         */
        private TimeUnit dequeListenerTimeoutUnit = TimeUnit.SECONDS;

        /**
         * 构造函数
         * @param inputClient 数据读取的客户端，使用需要迁移的es数据源构建
         * @param outputClient 数据输出客户端，使用迁移目标源的es数据源构建
         * @param indexArray 索引数组，需要同步数据的索引
         * @param dequeSize 阻塞队列大小
         * @param singleTransferSize 单次传输数量
         * @param isCustomOutputIndex 是否自定义输出索引 true - 是，false - 否, 默认值为false
         * @param dequeListenerTimeout 双端队列监听阻塞时长，单位秒
         */
        private DataSyncConfig(RestHighLevelClient inputClient, RestHighLevelClient outputClient, String[] indexArray, Integer dequeSize, Integer singleTransferSize, Boolean isCustomOutputIndex, Long dequeListenerTimeout){
            this.inputClient = inputClient;
            this.outputClient = outputClient;
            this.elasticsearchService = new ElasticsearchService();
            this.indexArray = indexArray;
            this.blockingDeque = new LinkedBlockingDeque<>(dequeSize);
            //设置单次传输的数据量
            this.singleTransferSize = singleTransferSize;
            //设置自定义输出索引标识符
            this.isCustomOutputIndex = isCustomOutputIndex;
            this.dequeListenerTimeout = dequeListenerTimeout;
            //数据统计数组，存储格式例子:
            // 非自定义输出索引 -> [index1,readTotal1,writeTotal1,index2,readTotal2,writeTotal2]
            // 自定义输出索引 -> [inputIndex, readTotal, outputIndex, writeTotal]
            this.totalCountArray = isCustomOutputIndex ? new Object[indexArray.length * 2] : new Object[indexArray.length * 3];
            //数据统计数组索引映射,记录每个索引名称在 totalCountArray 中的下标
            this.totalCountArrayIndexMap = new HashMap<>(indexArray.length);
        }


        /**
         * 构建线程池
         * @return
         */
        private ExecutorService buildExecutorService(Integer indexTaskNum){
            //核心线程数和最大线程数一样，都为索引任务数的双倍+1,如果索引任务只有一个，固定核心线程数为20
            Integer threadNum = indexTaskNum == 1 ? 20 : indexTaskNum * 2 + 1;
            ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum,
                    threadNum,
                    120,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(100),
                    new ThreadFactoryBuilder().setNameFormat("es-dump-pool-%d").build(),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            return executor;
        }

        /**
         * 设置是否需要自定义输出索引，default value false
         * @param customOutputIndex
         */
        public void setCustomOutputIndex(Boolean customOutputIndex) {
            this.isCustomOutputIndex = customOutputIndex;
        }

        /**
         * 设置单次传输的数据量，default value 500
         * @param singleTransferSize
         */
        public void setSingleTransferSize(Integer singleTransferSize){
            this.singleTransferSize = singleTransferSize;
        }

        public Integer getSingleTransferSize() {
            return singleTransferSize;
        }

        public Long getDequeListenerTimeout() {
            return dequeListenerTimeout;
        }

        public void setDequeListenerTimeout(Long dequeListenerTimeout) {
            this.dequeListenerTimeout = dequeListenerTimeout;
        }

        public TimeUnit getDequeListenerTimeoutUnit() {
            return dequeListenerTimeoutUnit;
        }

        public void setDequeListenerTimeoutUnit(TimeUnit dequeListenerTimeoutUnit) {
            this.dequeListenerTimeoutUnit = dequeListenerTimeoutUnit;
        }

        public RestHighLevelClient getInputClient() {
            return inputClient;
        }

        public RestHighLevelClient getOutputClient() {
            return outputClient;
        }

        public ElasticsearchService getElasticsearchService() {
            return elasticsearchService;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

        public LinkedBlockingDeque<ElasticsearchDataWrapper> getBlockingDeque() {
            return blockingDeque;
        }

        public String[] getIndexArray() {
            return indexArray;
        }

        public Map<String, Integer> getTotalCountArrayIndexMap() {
            return totalCountArrayIndexMap;
        }

        public Object[] getTotalCountArray() {
            return totalCountArray;
        }

        public Boolean getIsCustomOutputIndex(){
            return isCustomOutputIndex;
        }
    }
}
