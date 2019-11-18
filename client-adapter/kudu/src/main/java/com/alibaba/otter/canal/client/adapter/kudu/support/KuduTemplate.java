package com.alibaba.otter.canal.client.adapter.kudu.support;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　┻　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃  神兽保佑
 * 　　　　┃　　　┃  代码无bug
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Created by Liuyadong on 2019-11-12
 * kudu 操作工具类
 *
 * @description
 */
public class KuduTemplate {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private KuduClient kuduClient;
    private String masters;

    private final static int OPERATION_BATCH = 5000;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public KuduTemplate(String master_str) {
        this.masters = master_str;
        init();
        addShutdownHook();
    }

    public void init() {
        //kudu master 以逗号分隔
        List<String> masterList = Arrays.asList(masters.split(","));
        kuduClient = new KuduClient.KuduClientBuilder(masterList).defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(30000).defaultAdminOperationTimeoutMs(60000).build();
    }

    /**
     * 查询表是否存在
     *
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName) {
        try {
            return kuduClient.tableExists(tableName);
        } catch (KuduException e) {
            throw new RuntimeException("kudu table exists check fail");
        }
    }

    /**
     * 删除行
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void delete(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Delete delete = kuduTable.newDelete();
                PartialRow row = delete.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(delete);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    session.flush();
                    uncommit = 0;
                }
            }
            session.flush();
        } catch (KuduException e) {
            logger.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    /**
     * 更新字段
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void update(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Update update = kuduTable.newUpdate();
                PartialRow row = update.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(update);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    session.flush();
                    uncommit = 0;
                }
            }
            session.flush();
        } catch (KuduException e) {
            logger.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }


    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void insert(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);// 打开表
        KuduSession session = kuduClient.newSession();  // 创建写session,kudu必须通过session写入
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); // 采取Flush方式 手动刷新
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Insert insert = kuduTable.newInsert();
                PartialRow row = insert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(insert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    session.flush();
                    uncommit = 0;
                }
            }
            session.flush();
        } catch (KuduException e) {
            logger.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }

    }

    /**
     * 统计kudu表数据
     *
     * @param tableName
     * @return
     */
    public long countRow(String tableName) {
        long rowCount = 0L;
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            //创建scanner扫描
            KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();
            //遍历数据
            while (scanner.hasMoreRows()) {
                while (scanner.nextRows().hasNext()) {
                    rowCount++;
                }
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    /**
     * 关闭钩子
     *
     * @throws IOException
     */
    public void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (kuduClient != null) {
                    try {
                        kuduClient.close();
                    } catch (Exception e) {
                        logger.error("ShutdownHook Close KuduClient Error!", e);
                    }
                }
            }
        });
    }

    /**
     * 封装kudu行数据
     *
     * @param row
     * @param rawVal
     * @param type
     */
    private void fillRow(PartialRow row, String colName, Object rawVal, Type type) throws UnsupportedEncodingException {
        String rowValue = rawVal == null ? "0" : rawVal + "";
        switch (type) {
            case INT8:
                row.addByte(colName, Byte.parseByte(rowValue));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rowValue));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rowValue));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rowValue));
                break;
            case BINARY:
                rowValue = rawVal == null ? "" : rawVal + "";
                row.addBinary(colName, rowValue.getBytes());
                break;
            case STRING:
                rowValue = rawVal == null ? "" : rawVal + "";
                row.addString(colName, rowValue);
                break;
            case BOOL:
                rowValue = rawVal == null ? "true" : rawVal + "";
                row.addBoolean(colName, Boolean.parseBoolean(rowValue));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rowValue));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rowValue));
                break;
            case UNIXTIME_MICROS:
                if ("0".equals(rowValue)) {
                    row.addLong(colName, Long.parseLong(rowValue));
                } else {
                    try {
                        Date parse = sdf.parse(rowValue.substring(0, 19));
                        row.addLong(colName, parse.getTime());
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                }
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }
    }
}
