package com.alibaba.otter.canal.client.adapter.rdb.test;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.druid.pool.DruidDataSource;

@Ignore
public class DBTest {

    @Test
    public void test01() throws SQLException {
        DruidDataSource dataSource = new DruidDataSource();
        // dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
        // dataSource.setUrl("jdbc:oracle:thin:@127.0.0.1:49161:XE");
        // dataSource.setUsername("mytest");
        // dataSource.setPassword("m121212");

        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true");
        dataSource.setUsername("root");
        dataSource.setPassword("121212");

        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(2);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        dataSource.init();

        Connection conn = dataSource.getConnection();

        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement("insert into user (id,name,role_id,c_time,test1,test2) values (?,?,?,?,?,?)");

        java.util.Date now = new java.util.Date();
        for (int i = 1; i <= 10000; i++) {
            pstmt.clearParameters();
            pstmt.setLong(1, (long) i);
            pstmt.setString(2, "test_" + i);
            pstmt.setLong(3, (long) i % 4 + 1);
            pstmt.setDate(4, new java.sql.Date(now.getTime()));
            pstmt.setString(5, null);
            pstmt.setBytes(6, null);

            pstmt.execute();
            if (i % 5000 == 0) {
                conn.commit();
            }
        }
        conn.commit();

        pstmt.close();

        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("select * from user t where 1=2");
        //
        // ResultSetMetaData rsm = rs.getMetaData();
        // int cnt = rsm.getColumnCount();
        // for (int i = 1; i <= cnt; i++) {
        // System.out.println(rsm.getColumnName(i) + " " +
        // rsm.getColumnType(i));
        // }

        // rs.close();
        // stmt.close();

        // PreparedStatement pstmt = conn
        // .prepareStatement("insert into tb_user
        // (id,name,role_id,c_time,test1,test2)
        // values (?,?,?,?,?,?)");
        // pstmt.setBigDecimal(1, new BigDecimal("5"));
        // pstmt.setString(2, "test");
        // pstmt.setBigDecimal(3, new BigDecimal("1"));
        // pstmt.setDate(4, new Date(new java.util.Date().getTime()));
        // byte[] a = { (byte) 1, (byte) 2 };
        // pstmt.setBytes(5, a);
        // pstmt.setBytes(6, a);
        // pstmt.execute();
        //
        // pstmt.close();

        conn.close();
        dataSource.close();
    }

    @SuppressWarnings("unused")
    private String clob2Str(Clob clob) {
        String content = "";
        try (Reader is = clob.getCharacterStream(); BufferedReader buff = new BufferedReader(is)) {
            String line = buff.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                sb.append(line);
                line = buff.readLine();
            }
            content = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }
}
