package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.inbound.ParserExceptionHandler;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;

/**
 * @author chengjin.lyf on 2018/7/20 上午10:52
 * @since 1.0.25
 */
public class RdsBinlogEventParserProxy extends MysqlEventParser {

    private String                    rdsOpenApiUrl        = "https://rds.aliyuncs.com/";    // openapi地址
    private String                    accesskey;                                             // 云账号的ak
    private String                    secretkey;                                             // 云账号sk
    private String                    instanceId;                                            // rds实例id
    private Long                      startTime;
    private Long                      endTime;
    private String                    directory;                                             // binlog
                                                                                              // 目录
    private int                       batchSize            = 4;                              // 最多下载的binlog文件数量

    private RdsLocalBinlogEventParser rdsBinlogEventParser = new RdsLocalBinlogEventParser();
    private ExecutorService           executorService      = Executors.newSingleThreadExecutor(new ThreadFactory() {

                                                               @Override
                                                               public Thread newThread(Runnable r) {
                                                                   Thread t = new Thread(r, "rds-binlog-daemon-thread");
                                                                   t.setDaemon(true);
                                                                   return t;
                                                               }
                                                           });

    @Override
    public void start() {
        final ParserExceptionHandler targetHandler = this.getParserExceptionHandler();
        rdsBinlogEventParser.setLogPositionManager(this.getLogPositionManager());
        rdsBinlogEventParser.setDestination(destination);
        rdsBinlogEventParser.setAlarmHandler(this.getAlarmHandler());
        rdsBinlogEventParser.setConnectionCharset(this.connectionCharset);
        rdsBinlogEventParser.setConnectionCharsetNumber(this.connectionCharsetNumber);
        rdsBinlogEventParser.setEnableTsdb(this.enableTsdb);
        rdsBinlogEventParser.setEventBlackFilter(this.eventBlackFilter);
        rdsBinlogEventParser.setFilterQueryDcl(this.filterQueryDcl);
        rdsBinlogEventParser.setFilterQueryDdl(this.filterQueryDdl);
        rdsBinlogEventParser.setFilterQueryDml(this.filterQueryDml);
        rdsBinlogEventParser.setFilterRows(this.filterRows);
        rdsBinlogEventParser.setFilterTableError(this.filterTableError);
        rdsBinlogEventParser.setIsGTIDMode(this.isGTIDMode);
        rdsBinlogEventParser.setMasterInfo(this.masterInfo);
        rdsBinlogEventParser.setEventFilter(this.eventFilter);
        rdsBinlogEventParser.setMasterPosition(this.masterPosition);
        rdsBinlogEventParser.setTransactionSize(this.transactionSize);
        rdsBinlogEventParser.setUrl(this.rdsOpenApiUrl);
        rdsBinlogEventParser.setAccesskey(this.accesskey);
        rdsBinlogEventParser.setSecretkey(this.secretkey);
        rdsBinlogEventParser.setInstanceId(this.instanceId);
        rdsBinlogEventParser.setEventSink(eventSink);
        rdsBinlogEventParser.setDirectory(directory);
        rdsBinlogEventParser.setBatchSize(batchSize);
        rdsBinlogEventParser.setFinishListener(new RdsLocalBinlogEventParser.ParseFinishListener() {

            @Override
            public void onFinish() {
                executorService.execute(new Runnable() {

                    @Override
                    public void run() {
                        rdsBinlogEventParser.stop();
                        RdsBinlogEventParserProxy.this.start();
                    }
                });

            }
        });
        this.setParserExceptionHandler(new ParserExceptionHandler() {

            @Override
            public void handle(Throwable e) {
                handleMysqlParserException(e);
                if (targetHandler != null) {
                    targetHandler.handle(e);
                }
            }
        });
        super.start();
    }

    public void handleMysqlParserException(Throwable throwable) {
        if (throwable instanceof PositionNotFoundException) {
            logger.info("remove rds not found position, try download rds binlog!");
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        logger.info("stop mysql parser!");
                        RdsBinlogEventParserProxy rdsBinlogEventParserProxy = RdsBinlogEventParserProxy.this;
                        long serverId = rdsBinlogEventParserProxy.getServerId();
                        rdsBinlogEventParser.setServerId(serverId);
                        rdsBinlogEventParserProxy.stop();
                        logger.info("start rds mysql binlog parser!");
                        rdsBinlogEventParser.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public boolean isStart() {
        return super.isStart();
    }

    public void setRdsOpenApiUrl(String rdsOpenApiUrl) {
        this.rdsOpenApiUrl = rdsOpenApiUrl;
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public void setSecretkey(String secretkey) {
        this.secretkey = secretkey;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
