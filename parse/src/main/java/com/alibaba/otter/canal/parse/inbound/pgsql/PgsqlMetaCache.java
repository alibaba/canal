package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.RowMapper;

public class PgsqlMetaCache {

  private final PgsqlConnection pgsqlConnection;
  private final LoadingCache<String, LoadingCache<String, TableMeta>> cache;

  public PgsqlMetaCache(PgsqlConnection pgsqlConnection, int timeoutInSeconds) {
    this.pgsqlConnection = pgsqlConnection;
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(timeoutInSeconds, TimeUnit.SECONDS)
        .build(new CacheLoader<String, LoadingCache<String, TableMeta>>() {
          @Override
          public LoadingCache<String, TableMeta> load(String schema) throws Exception {
            return CacheBuilder.newBuilder()
                .expireAfterWrite(timeoutInSeconds, TimeUnit.SECONDS)
                .build(new CacheLoader<String, TableMeta>() {
                  @Override
                  public TableMeta load(String table) throws Exception {
                    return loadTableMetaWithRetry(schema, table);
                  }
                });
          }
        });
  }

  public TableMeta getTableMeta(String schema, String table) {
    return this.cache.getUnchecked(schema).getUnchecked(table);
  }

  public void clear() {
    this.cache.asMap().values().forEach(Cache::cleanUp);
    this.cache.cleanUp();
  }

  public void reload(String schema, String table) {
    this.cache.refresh(schema);
    this.cache.getUnchecked(schema).refresh(table);
  }

  private String getFullName(String schema, String table) {
    return "\"" + schema + "\".\"" + table + "\"";
  }

  private TableMeta loadTableMetaWithRetry(String schema, String tableName) {
    try {
      return loadTableMeta(schema, tableName);
    } catch (Throwable e) {
      throw new CanalParseException("fetch failed by table meta:" + getFullName(schema, tableName), e);
    }
  }

  private TableMeta loadTableMeta(String schema, String table) throws IOException {
    String pkColumnsSql = "SELECT c.column_name AS column_name FROM information_schema.table_constraints tc "
        + "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) "
        + "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name "
        + "WHERE constraint_type = 'PRIMARY KEY' and tc.table_schema='" + schema + "' and tc.table_name = '" + table
        + "';";

    String columnsSql =
        "select * from information_schema.columns where table_schema='" + schema + "' and table_name='" + table + "';";

    List<String> pks = this.pgsqlConnection.queryMulti(pkColumnsSql, new RowMapper<String>() {
      @Override
      public String mapRow(ResultSet rs, int rowNum) throws SQLException {
        return rs.getString("column_name");
      }
    });

    List<FieldMeta> fieldMetas = this.pgsqlConnection.queryMulti(columnsSql, new RowMapper<FieldMeta>() {
      @Override
      public FieldMeta mapRow(ResultSet rs, int i) throws SQLException {
        String columnName = rs.getString("column_name");
        String columnType = rs.getString("data_type");
        String columnDefault = rs.getString("column_default");
        boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
        boolean key = pks.contains(columnName);
        return new FieldMeta(columnName, columnType, nullable, key, columnDefault);
      }
    });

    TableMeta tableMeta = new TableMeta();
    tableMeta.setSchema(schema);
    tableMeta.setTable(table);
    tableMeta.setDdl("");
    tableMeta.setFields(fieldMetas);
    return tableMeta;
  }

}
