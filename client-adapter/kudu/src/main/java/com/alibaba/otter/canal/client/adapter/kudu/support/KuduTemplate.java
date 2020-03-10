package com.alibaba.otter.canal.client.adapter.kudu.support;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuyadong
 * @description kudu 操作工具类
 */
public class KuduTemplate {

    private Logger           logger          = LoggerFactory.getLogger(this.getClass());

    private KuduClient       kuduClient;
    private String           masters;

    private final static int OPERATION_BATCH = 500;

    private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public KuduTemplate(String master_str){
        this.masters = master_str;
        checkClient();
    }

    /**
     * 检车连接
     */
    private void checkClient() {
        if (kuduClient == null) {
            // kudu master 以逗号分隔
            List<String> masterList = Arrays.asList(masters.split(","));
            kuduClient = new KuduClient.KuduClientBuilder(masterList).defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(60000)
                .defaultAdminOperationTimeoutMs(60000)
                .build();
        }
    }

    /**
     * 查询表是否存在
     *
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName) {
        this.checkClient();
        try {
            return kuduClient.tableExists(tableName);
        } catch (KuduException e) {
            logger.error("kudu table exists check fail,message :{}", e.getMessage());
            return true;
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
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            // 获取元数据结构
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
                    fillRow(row, name, value, type); // 填充行数据
                }
                session.apply(delete);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> delete_option = session.flush();
                    if (delete_option.size() > 0) {
                        OperationResponse response = delete_option.get(0);
                        if (response.hasRowError()) {
                            logger.error("delete row fail table name is :{} ", tableName);
                            logger.error("error list is :{}", response.getRowError().getMessage());
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> delete_option = session.flush();
            if (delete_option.size() > 0) {
                OperationResponse response = delete_option.get(0);
                if (response.hasRowError()) {
                    logger.error("delete row fail table name is :{} ", tableName);
                    logger.error("error list is :{}", response.getRowError().getMessage());
                }
            }

        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    /**
     * 更新/插入字段
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void upsert(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            // 获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Upsert upsert = kuduTable.newUpsert();
                PartialRow row = upsert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); // 填充行数据
                }
                session.apply(upsert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> update_option = session.flush();
                    if (update_option.size() > 0) {
                        OperationResponse response = update_option.get(0);
                        if (response.hasRowError()) {
                            logger.error("update row fail table name is :{} ", tableName);
                            logger.error("update list is :{}", response.getRowError().getMessage());
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> update_option = session.flush();
            if (update_option.size() > 0) {
                OperationResponse response = update_option.get(0);
                if (response.hasRowError()) {
                    logger.error("update row fail table name is :{} ", tableName);
                    logger.error("update list is :{}", response.getRowError().getMessage());
                }
            }
        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
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
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);// 打开表
        KuduSession session = kuduClient.newSession(); // 创建写session,kudu必须通过session写入
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); // 采取Flush方式
                                                                               // 手动刷新
            session.setMutationBufferSpace(OPERATION_BATCH);
            // 获取元数据结构
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
                    fillRow(row, name, value, type); // 填充行数据
                }
                session.apply(insert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> insert_option = session.flush();
                    if (insert_option.size() > 0) {
                        OperationResponse response = insert_option.get(0);
                        if (response.hasRowError()) {
                            logger.error("insert row fail table name is :{} ", tableName);
                            logger.error("insert list is :{}", response.getRowError().getMessage());
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> insert_option = session.flush();
            if (insert_option.size() > 0) {
                OperationResponse response = insert_option.get(0);
                if (response.hasRowError()) {
                    logger.error("insert row fail table name is :{} ", tableName);
                    logger.error("insert list is :{}", response.getRowError().getMessage());
                }
            }
        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
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
        this.checkClient();
        long rowCount = 0L;
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            // 创建scanner扫描
            KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();
            // 遍历数据
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
    public void closeKuduClient() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (Exception e) {
                logger.error("ShutdownHook Close KuduClient Error! error message {}", e.getMessage());
            }
        }
    }

    /**
     * 封装kudu行数据
     *
     * @param row
     * @param rawVal
     * @param type
     */
    private void fillRow(PartialRow row, String colName, Object rawVal, Type type) {
        String rowValue = "0";
        if (!(rawVal == null || "".equals(rawVal))) {
            rowValue = rawVal + "";
        } else {
            return;
        }
        if (type == null) {
            logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
            return;
        }
        try {
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
                    row.addBinary(colName, rowValue.getBytes());
                    break;
                case STRING:
                    row.addString(colName, rowValue);
                    break;
                case BOOL:
                    if (!("true".equalsIgnoreCase(rowValue) || "false".equalsIgnoreCase(rowValue))) {
                        return;
                    }
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
                        try {
                            Date parse = sdf.parse("2099-11-11 11:11:11");
                            row.addLong(colName, parse.getTime());
                        } catch (ParseException e) {
                            logger.warn("date column is null");
                        }
                    } else {
                        try {
                            Date parse = rowValue.length() > 19 ? sdf.parse(rowValue.substring(0, 19)) : sdf.parse(rowValue);
                            row.addLong(colName, parse.getTime());
                        } catch (ParseException e) {
                            logger.warn("date format error, error data is :{}", rowValue);
                            try {
                                Date parse = sdf.parse("2099-11-11 11:11:11");
                                row.addLong(colName, parse.getTime());
                            } catch (ParseException ie) {
                                logger.warn("date column is null");
                            }
                        }
                    }
                    break;
                default:
                    logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
            }
        } catch (NumberFormatException e) {
            logger.error(e.getMessage());
            logger.error("column parse fail==> column name=>{},column type=>{},column value=>{}", colName, type, rawVal);
        }
    }

    /**
     * kudu数据类型映射
     *
     * @param
     */
    private Type toKuduType(String mysqlType) throws IllegalArgumentException {

        switch (mysqlType) {
            case "varchar":
                return Type.STRING;
            case "int":
                return Type.INT8;
            case "decimal":
                return Type.DOUBLE;
            case "double":
                return Type.DOUBLE;
            case "datetime":
                return Type.STRING;
            case "timestamp":
                return Type.STRING;
            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }
}
