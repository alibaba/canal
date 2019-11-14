package com.alibaba.otter.canal.client.adapter.kudu.support;

import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
     * @param columnTypeMap
     * @throws KuduException
     */
    public void delete(String tableName, List<Map<String, Object>> dataList, Map<String, Integer> columnTypeMap) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(3000);
        for (Map<String, Object> data : dataList) {
            Delete delete = kuduTable.newDelete();
            PartialRow row = delete.getRow();
            for (Map.Entry<String, Object> dataLine : data.entrySet()) {
                String columnName = dataLine.getKey();
                Object value = dataLine.getValue();
                Integer columnType = columnTypeMap.get(columnName.toLowerCase());
                //填充行数据
                fillRow(row, columnName, value, columnType);
            }
            session.flush();
            session.apply(delete);
        }
        session.close();

    }

    /**
     * 更新字段
     *
     * @param tableName
     * @param dataList
     * @param columnTypeMap
     * @throws KuduException
     */
    public void update(String tableName, List<Map<String, Object>> dataList, Map<String, Integer> columnTypeMap) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(3000);
        for (Map<String, Object> data : dataList) {
            Update update = kuduTable.newUpdate();
            PartialRow row = update.getRow();
            for (Map.Entry<String, Object> dataLine : data.entrySet()) {
                String columnName = dataLine.getKey();
                Object value = dataLine.getValue();
                Integer columnType = columnTypeMap.get(columnName.toLowerCase());
                //填充行数据
                fillRow(row, columnName, value, columnType);
            }
            session.flush();
            session.apply(update);
        }
        session.close();

    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param dataList
     * @param columnTypeMap
     * @throws KuduException
     */
    public void insert(String tableName, List<Map<String, Object>> dataList, Map<String, Integer> columnTypeMap) throws KuduException {
        // 打开表
        KuduTable table = kuduClient.openTable(tableName);
        // 创建写session,kudu必须通过session写入
        KuduSession session = kuduClient.newSession();
        // 采取Flush方式 手动刷新
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(3000);
        for (Map<String, Object> data : dataList) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            for (Map.Entry<String, Object> dataLine : data.entrySet()) {
                String columnName = dataLine.getKey().toLowerCase();
                Object value = dataLine.getValue();
                Integer columnType = columnTypeMap.get(columnName.toLowerCase());
                //填充行数据
                fillRow(row, columnName, value, columnType);
            }
            session.flush();
            session.apply(insert);
        }
        session.close();
    }


    /**
     * 封装kudu行数据
     *
     * @param row
     * @param columnName
     * @param value
     * @param columnType
     */
    private void fillRow(PartialRow row, String columnName, Object value, Integer columnType) {
        switch (columnType) {
            case Types.BIT:
            case Types.BOOLEAN:
                if (value instanceof Boolean) {
                    row.addBoolean(columnName, (Boolean) value);
                } else if (value instanceof String) {
                    boolean v = !value.equals("0");
                    row.addBoolean(columnName, v);
                } else if (value instanceof Number) {
                    boolean v = ((Number) value).intValue() != 0;
                    row.addBoolean(columnName, v);
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (value instanceof String) {
                    row.addString(columnName, (String) value);
                } else if (value == null) {
                    row.setNull(columnName);
                } else {
                    row.addString(columnName, value.toString());
                }
                break;
            case Types.TINYINT:
                if (value instanceof Number) {
                    row.addByte(columnName, ((Number) value).byteValue());
                } else if (value instanceof String) {
                    row.addByte(columnName, Byte.parseByte((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.SMALLINT:
                if (value instanceof Number) {
                    row.addShort(columnName, ((Number) value).shortValue());
                } else if (value instanceof String) {
                    row.addShort(columnName, Short.parseShort((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.INTEGER:
                if (value instanceof Number) {
                    row.addInt(columnName, ((Number) value).intValue());
                } else if (value instanceof String) {
                    row.addInt(columnName, Integer.parseInt((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.BIGINT:
                if (value instanceof Number) {
                    row.addLong(columnName, ((Number) value).longValue());
                } else if (value instanceof String) {
                    row.addLong(columnName, Long.parseLong((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (value instanceof BigDecimal) {
                    row.addDouble(columnName, ((BigDecimal) value).doubleValue());
                } else if (value instanceof Byte) {
                    row.addInt(columnName, ((Byte) value).intValue());
                } else if (value instanceof Short) {
                    row.addInt(columnName, ((Short) value).intValue());
                } else if (value instanceof Integer) {
                    row.addInt(columnName, (Integer) value);
                } else if (value instanceof Long) {
                    row.addLong(columnName, (Long) value);
                } else if (value instanceof Float) {
                    row.addDouble(columnName, (float) ((Float) value).doubleValue());
                } else if (value instanceof Double) {
                    row.addDouble(columnName, (double) value);
                } else if (value != null) {
                    row.addDouble(columnName, Double.valueOf(value.toString()));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.REAL:
                if (value instanceof Number) {
                    row.addFloat(columnName, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    row.addFloat(columnName, Float.parseFloat((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Number) {
                    row.addDouble(columnName, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    row.addDouble(columnName, Double.parseDouble((String) value));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                if (value instanceof Blob) {
                    try {
                        int length = (int) ((Blob) value).length();
                        row.addBinary(columnName, ((Blob) value).getBytes(1, length));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (value instanceof byte[]) {
                    row.addBinary(columnName, (byte[]) value);
                } else if (value instanceof String) {
                    row.addBinary(columnName, ((String) value).getBytes(StandardCharsets.ISO_8859_1));
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.CLOB:
                if (value instanceof Clob) {
                    String detailinfo = null;
                    try {
                        detailinfo = "";
                        Clob detail = (Clob) value;
                        int i = 0;
                        if (value != null) {
                            InputStream input = detail.getAsciiStream();
                            int len = (int) detail.length();
                            byte by[] = new byte[len];
                            while (-1 != (i = input.read(by, 0, by.length))) {
                                input.read(by, 0, i);
                            }
                            detailinfo = new String(by, "utf-8");
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    row.addBinary(columnName, Bytes.fromString(detailinfo));
                } else if (value instanceof byte[]) {
                    row.addBinary(columnName, (byte[]) value);
                } else if (value instanceof String) {
                    row.addBinary(columnName, ((String) value).getBytes());
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.DATE:
                if (value instanceof java.sql.Date) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
                    row.addString(columnName, dateFmt.format((java.sql.Date) value));
                } else if (value instanceof java.util.Date) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
                    row.addString(columnName, dateFmt.format(value));
                } else if (value instanceof String) {
                    row.addString(columnName, (String) value);
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.TIME:
                if (value instanceof java.sql.Time) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("HH:mm:ss");
                    row.addString(columnName, dateFmt.format((java.sql.Time) value));
                } else if (value instanceof java.util.Date) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("HH:mm:ss");
                    row.addString(columnName, dateFmt.format(value));
                } else if (value instanceof String) {
                    row.addString(columnName, (String) value);
                } else {
                    row.setNull(columnName);
                }
                break;
            case Types.TIMESTAMP:
                if (value instanceof java.sql.Timestamp) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    row.addString(columnName, dateFmt.format((java.sql.Timestamp) value));
                } else if (value instanceof java.util.Date) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    row.addString(columnName, dateFmt.format(value));
                } else if (value instanceof String) {
                    row.addString(columnName, (String) value);
                } else {
                    row.setNull(columnName);
                }
                break;
            default:
                row.setNull(columnName);
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
     * 关闭kudu client
     */
    public void close() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (Exception e) {
                logger.error("Close KuduClient Error!", e);
            }
        }
    }
}
