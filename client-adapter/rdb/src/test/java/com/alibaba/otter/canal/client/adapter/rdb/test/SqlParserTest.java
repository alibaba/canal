//wpackage com.alibaba.otter.canal.client.adapter.rdb.test;
//
//import com.alibaba.druid.sql.ast.SQLName;
//import com.alibaba.druid.sql.ast.SQLStatement;
//import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
//import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
//import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
//import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
//import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
//import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
//import com.alibaba.druid.sql.parser.SQLStatementParser;
//
//import java.io.StringWriter;
//
//public class SqlParserTest {
//
//    public static class TableNameVisitor extends MySqlOutputVisitor {
//
//        public TableNameVisitor(Appendable appender){
//            super(appender);
//        }
//
//        @Override
//        public boolean visit(SQLExprTableSource x) {
//            SQLName table = (SQLName) x.getExpr();
//            String tableName = table.getSimpleName();
//
//            // 改写tableName
//            print0("new_" + tableName.toUpperCase());
//
//            return true;
//        }
//
//    }
//
//    public static void main(String[] args) {
//        // String sql = "select * from `mytest`.`t` where id=1 and name=ming group by
//        // uid limit 1,200 order by ctime";
//
//        String sql = "CREATE TABLE `mytest`.`user` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
//                     + "  `name` varchar(30) NOT NULL,\n" + "  `c_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
//                     + "  `role_id` bigint(20) DEFAULT NULL,\n" + "  `test1` text,\n" + "  `test2` blob,\n"
//                     + "  `key` varchar(30) DEFAULT NULL,\n" + "  PRIMARY KEY (`id`)\n"
//                     + ") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;";
//
//        // // 新建 MySQL Parser
//        // SQLStatementParser parser = new MySqlStatementParser(sql);
//        //
//        // // 使用Parser解析生成AST，这里SQLStatement就是AST
//        // SQLStatement sqlStatement = parser.parseStatement();
//        //
//        // MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
//        // sqlStatement.accept(visitor);
//        //
//        // System.out.println("getTables:" + visitor.getTables());
//        // System.out.println("getParameters:" + visitor.getParameters());
//        // System.out.println("getOrderByColumns:" + visitor.getOrderByColumns());
//        // System.out.println("getGroupByColumns:" + visitor.getGroupByColumns());
//        // System.out.println("---------------------------------------------------------------------------");
//        //
//        // // 使用select访问者进行select的关键信息打印
//        // // SelectPrintVisitor selectPrintVisitor = new SelectPrintVisitor();
//        // // sqlStatement.accept(selectPrintVisitor);
//        //
//        // System.out.println("---------------------------------------------------------------------------");
//        // // 最终sql输出
//        // StringWriter out = new StringWriter();
//        // TableNameVisitor outputVisitor = new TableNameVisitor(out);
//        // sqlStatement.accept(outputVisitor);
//        // System.out.println(out.toString());
//
//        MySqlCreateTableParser parser1 = new MySqlCreateTableParser(sql);
//        SQLCreateTableStatement createTableStatement = parser1.parseCreateTable();
////        MySqlSchemaStatVisitor visitor1 = new MySqlSchemaStatVisitor();
////        createTableStatement.accept(visitor1);
//        // visitor1.getTables().forEach((k, v) -> {
//        // System.out.println(k.);
//        // System.out.println(v);
//        // });
//        // 最终sql输出
//        StringWriter out = new StringWriter();
//        TableNameVisitor outputVisitor = new TableNameVisitor(out);
//        createTableStatement.accept(outputVisitor);
//        System.out.println(out.toString());
//    }
//}
