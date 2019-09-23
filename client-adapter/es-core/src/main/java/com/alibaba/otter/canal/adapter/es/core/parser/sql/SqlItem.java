package com.alibaba.otter.canal.adapter.es.core.parser.sql;

import java.util.*;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;

public class SqlItem {
    private Map<String, FieldItem> selectFields = new LinkedHashMap<>();  //alias -> field

    private Map<String, TableItem> tables = new LinkedHashMap<>();        //alias -> table

    private WhereItem whereItem = null;

    private GroupByItem groupByItem = null;

    public Map<String, FieldItem> getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(Map<String, FieldItem> selectFields) {
        this.selectFields = selectFields;
    }

    public Map<String, TableItem> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableItem> tables) {
        this.tables = tables;
    }

    public WhereItem getWhereItem() {
        return whereItem;
    }

    public void setWhereItem(WhereItem whereItem) {
        this.whereItem = whereItem;
    }

    public GroupByItem getGroupByItem() {
        return groupByItem;
    }

    public void setGroupByItem(GroupByItem groupByItem) {
        this.groupByItem = groupByItem;
    }

    private Boolean isAllFieldsSimple = null;

    /**
     * 是否所有select字段为可执行简单字段
     *
     * @return true/false
     */
    public Boolean isAllFieldsSimple() {
        if (isAllFieldsSimple != null) {
            return isAllFieldsSimple;
        }
        boolean res = true;
        for (FieldItem fieldItem : getSelectFields().values()) {
            if (!fieldItem.isSimple()) {
                res = false;
                break;
            }
        }
        isAllFieldsSimple = res;
        return isAllFieldsSimple;
    }

    private Boolean isConvertable = null;

    /**
     * 判断sql是否可以转换
     *
     * @return true/false
     */
    public Boolean isConvertable() {
        if (isConvertable != null) {
            return isConvertable;
        }
        boolean res = isAllFieldsSimple();
        res &= getWhereItem() == null || getWhereItem().isSimple;
        res &= getGroupByItem() == null;
        isConvertable = res;
        return isConvertable;
    }

    private String selectSQL = null;

    public String selectSQL() {
        if (selectSQL != null) {
            return selectSQL;
        }
        StringBuilder sb = new StringBuilder("SELECT ");
        int i = 0;
        for (FieldItem fieldItem : this.selectFields.values()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(fieldItem.getExpr());
        }
        selectSQL = sb.toString();
        return selectSQL;
    }

    private String fromSQL = null;

    public String fromSQL() {
        if (fromSQL != null) {
            return fromSQL;
        }
        StringBuilder sb = new StringBuilder(" FROM ");
        int i = 0;
        for (TableItem tableItem : this.tables.values()) {
            if (i != 0) {
                switch (tableItem.getJoinType()) {
                    case TableItem.INNER_JOIN: {
                        sb.append(" INNER JOIN ");
                        break;
                    }
                    case TableItem.RIGHT_JOIN: {
                        sb.append(" RIGHT JOIN ");
                        break;
                    }
                    case TableItem.LEFT_JOIN:
                    default: {
                        sb.append(" LEFT JOIN ");
                        break;
                    }
                }
            }
            if (tableItem instanceof SubQueryItem) {
                SubQueryItem subQueryItem = (SubQueryItem) tableItem;
                sb.append("(").append(subQueryItem.getSubQueryBlock()).append(") ").append(tableItem.getAlias());
            } else {
                sb.append(tableItem.getTableName()).append(" ").append(tableItem.getAlias());
            }
            if (i != 0) {
                sb.append(" ON ");
                for (ConditionPair onFields : tableItem.getOnFields()) {
                    sb.append(onFields.getExpr());
                }
            }
            i++;
        }
        fromSQL = sb.toString();
        return fromSQL;
    }

    private String whereSQL = null;

    public String whereSQL() {
        if (whereSQL != null) {
            return whereSQL;
        }
        StringBuilder sb = new StringBuilder();
        if (whereItem != null) {
            sb.append(" WHERE ").append(whereItem.getExpr());
        }
        whereSQL = sb.toString();
        return whereSQL;
    }

    private String groupBySQL = null;

    public String groupBySQL() {
        if (groupBySQL != null) {
            return groupBySQL;
        }
        StringBuilder sb = new StringBuilder();
        if (groupByItem != null) {
            sb.append(" ").append(groupByItem.getExpr());
        }
        groupBySQL = sb.toString();
        return groupBySQL;
    }

    private String formattedSQL = null;

    public String formattedSQL() {
        if (formattedSQL != null) {
            return formattedSQL;
        }
        String sql = selectSQL() + fromSQL() + whereSQL() + groupBySQL();
        formattedSQL = sql.replace("\n\t", " ").replace('\n', ' ').replace('\t', ' ');
        return formattedSQL;
    }

    @Override
    public String toString() {
        return "SqlItem{" +
                "selectFields=" + selectFields +
                ", tables=" + tables +
                ", whereItem=" + whereItem +
                ", groupByItem=" + groupByItem +
                ", isAllFieldsSimple=" + isAllFieldsSimple +
                ", isConvertable=" + isConvertable +
                ", selectSQL='" + selectSQL + '\'' +
                ", fromSQL='" + fromSQL + '\'' +
                ", whereSQL='" + whereSQL + '\'' +
                ", groupBySQL='" + groupBySQL + '\'' +
                ", formattedSQL='" + formattedSQL + '\'' +
                '}';
    }

    public static class SubQueryItem extends TableItem {
        private SQLSelectQueryBlock subQueryBlock;

        private List<FieldItem> selectFields = new ArrayList<>();

        private WhereItem whereItem = null;

        private GroupByItem groupByItem = null;

        private Boolean isAllFieldsSimple = null;

        /**
         * 判断所有select字段为可执行简单字段
         *
         * @return true/false
         */
        public boolean isAllFieldsSimple() {
            if (isAllFieldsSimple != null) {
                return isAllFieldsSimple;
            }
            boolean res = true;
            for (FieldItem fieldItem : getSelectFields()) {
                if (!fieldItem.isSimple()) {
                    res = false;
                    break;
                }
            }
            isAllFieldsSimple = res;
            return isAllFieldsSimple;
        }

        private Boolean isConvertable = null;

        /**
         * 判断sql是否可以转换
         *
         * @return true/false
         */
        public boolean isConvertable() {
            if (isConvertable != null) {
                return isConvertable;
            }
            boolean res = isAllFieldsSimple();
            res &= getWhereItem() == null || getWhereItem().isSimple;
            res &= getGroupByItem() == null;
            isConvertable = res;
            return isConvertable;
        }


        public SQLSelectQueryBlock getSubQueryBlock() {
            return subQueryBlock;
        }

        public void setSubQueryBlock(SQLSelectQueryBlock subQueryBlock) {
            this.subQueryBlock = subQueryBlock;
        }

        public List<FieldItem> getSelectFields() {
            return selectFields;
        }

        public void setSelectFields(List<FieldItem> selectFields) {
            this.selectFields = selectFields;
        }

        public WhereItem getWhereItem() {
            return whereItem;
        }

        public void setWhereItem(WhereItem whereItem) {
            this.whereItem = whereItem;
        }

        public GroupByItem getGroupByItem() {
            return groupByItem;
        }

        public void setGroupByItem(GroupByItem groupByItem) {
            this.groupByItem = groupByItem;
        }

        @Override
        public String toString() {
            return "SubQueryItem{" +
                    "subQueryBlock=" + subQueryBlock +
                    ", selectFields=" + selectFields +
                    ", whereItem=" + whereItem +
                    ", groupByItem=" + groupByItem +
                    ", isAllFieldsSimple=" + isAllFieldsSimple +
                    ", isConvertable=" + isConvertable +
                    '}';
        }
    }

    public static class TableItem {
        public final static int INNER_JOIN = 1, LEFT_JOIN = 2, RIGHT_JOIN = 3;

        private String schema;
        private String tableName;
        private String alias;
        private int joinType = LEFT_JOIN;
        private List<ConditionPair> onFields = new ArrayList<>();           //on条件

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public int getJoinType() {
            return joinType;
        }

        public void setJoinType(int joinType) {
            this.joinType = joinType;
        }

        public List<ConditionPair> getOnFields() {
            return onFields;
        }

        public void setOnFields(List<ConditionPair> onFields) {
            this.onFields = onFields;
        }

        @Override
        public String toString() {
            return "TableItem{" +
                    "schema='" + schema + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", alias='" + alias + '\'' +
                    ", joinType=" + joinType +
                    ", onFields=" + onFields +
                    '}';
        }
    }

    public static class FieldItem {
        private String fieldName;
        private SQLExpr expr;
        private boolean isSimple = true;                               //是否简单字段(包括可执行函数或可执行二元运算)
        private boolean isRaw = true;                                  //是否原生字段
        private Set<ColumnItem> columnItems = new LinkedHashSet<>();
        private Set<String> owners = new LinkedHashSet<>();


        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public SQLExpr getExpr() {
            return expr;
        }

        public void setExpr(SQLExpr expr) {
            this.expr = expr;
        }

        public boolean isSimple() {
            return isSimple;
        }

        public void setSimple(boolean simple) {
            isSimple = simple;
        }

        public boolean isRaw() {
            return isRaw;
        }

        public void setRaw(boolean raw) {
            isRaw = raw;
        }

        public Set<ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(Set<ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public Set<String> getOwners() {
            return owners;
        }

        public void setOwners(Set<String> owners) {
            this.owners = owners;
        }

        private String rawColumn = null;

        public String getRawColumn() {
            if (rawColumn != null) {
                return rawColumn;
            }
            if (isRaw && !columnItems.isEmpty()) {
                rawColumn = columnItems.iterator().next().columnName;
                return rawColumn;
            }
            return null;
        }

        private String rawOwner = null;

        public String getRawOwner() {
            if (rawOwner != null) {
                return rawOwner;
            }
            if (isRaw && !columnItems.isEmpty()) {
                rawOwner = columnItems.iterator().next().owner;
                return rawOwner;
            }
            return null;
        }

        private String rawOwnerColumn = null;

        public String getRawOwnerColumn() {
            if (rawOwnerColumn != null) {
                return rawOwnerColumn;
            }
            if (isRaw && !columnItems.isEmpty()) {
                ColumnItem columnItem = columnItems.iterator().next();
                String owner = "";
                if (columnItem.getOwner() != null && !columnItem.getOwner().equals("")) {
                    owner = columnItem.getOwner() + ".";
                }
                rawOwnerColumn = owner + columnItem.getColumnName();
            }
            return null;
        }

        private Boolean isSingle = null;

        /**
         * 判断字段中是否只包含一个owner
         *
         * @return true/false
         */
        public boolean isSingle() {
            if (isSingle != null) {
                return isSingle;
            }
            Set<String> owners = new HashSet<>(columnItems.size());
            for (ColumnItem columnItem : columnItems) {
                owners.add(columnItem.getOwner().toLowerCase());
            }
            isSingle = owners.size() <= 1;
            return isSingle;
        }

        @Override
        public String toString() {
            return "FieldItem{" +
                    "fieldName='" + fieldName + '\'' +
                    ", expr=" + expr +
                    ", isSimple=" + isSimple +
                    ", isRaw=" + isRaw +
                    ", columnItems=" + columnItems +
                    ", owners=" + owners +
                    ", rawColumn='" + rawColumn + '\'' +
                    ", rawOwner='" + rawOwner + '\'' +
                    ", rawOwnerColumn='" + rawOwnerColumn + '\'' +
                    ", isSingle=" + isSingle +
                    '}';
        }
    }

    public static class ConditionPair {
        private SQLExpr expr;
        private FieldItem left;
        private FieldItem right;

        public ConditionPair(FieldItem left, FieldItem right) {
            this.left = left;
            this.right = right;
        }

        public FieldItem getLeft() {
            return left;
        }

        public void setLeft(FieldItem left) {
            this.left = left;
        }

        public FieldItem getRight() {
            return right;
        }

        public void setRight(FieldItem right) {
            this.right = right;
        }

        public SQLExpr getExpr() {
            return expr;
        }

        public void setExpr(SQLExpr expr) {
            this.expr = expr;
        }

        public FieldItem getSelfField(String owner) {
            if (left.getRawOwner().equalsIgnoreCase(owner)) {
                return left;
            } else if (right.getRawOwner().equalsIgnoreCase(owner)) {
                return right;
            } else {
                return null;
            }
        }

        public FieldItem getOtherField(String owner) {
            if (left.getRawOwner().equalsIgnoreCase(owner)) {
                return right;
            } else if (right.getRawOwner().equalsIgnoreCase(owner)) {
                return left;
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return "ConditionPair{" +
                    "expr=" + expr +
                    ", left=" + left +
                    ", right=" + right +
                    '}';
        }
    }

    public static class ColumnItem {
        private String owner;
        private String columnName;

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        private String ownerColumn = null;

        public String getOwnerColumn() {
            if (ownerColumn != null) {
                return ownerColumn;
            }
            String owner = "";
            if (getOwner() != null && !getOwner().equals("")) {
                owner = getOwner() + ".";
            }
            ownerColumn = owner + getColumnName();
            return ownerColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnItem that = (ColumnItem) o;

            if (!Objects.equals(owner, that.owner)) return false;
            return Objects.equals(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            int result = owner != null ? owner.hashCode() : 0;
            result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ColumnItem{" +
                    "owner='" + owner + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", ownerColumn='" + ownerColumn + '\'' +
                    '}';
        }
    }

    public static class WhereItem {
        private SQLExpr expr;
        private boolean isSimple = true;
        private Set<ColumnItem> columnItems = new LinkedHashSet<>();
        private Set<String> owners = new LinkedHashSet<>();

        public SQLExpr getExpr() {
            return expr;
        }

        public void setExpr(SQLExpr expr) {
            this.expr = expr;
        }

        public boolean isSimple() {
            return isSimple;
        }

        public void setSimple(boolean simple) {
            isSimple = simple;
        }

        public Set<ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(Set<ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public Set<String> getOwners() {
            return owners;
        }

        public void setOwners(Set<String> owners) {
            this.owners = owners;
        }

        @Override
        public String toString() {
            return "WhereItem{" +
                    "expr=" + expr +
                    ", isSimple=" + isSimple +
                    ", columnItems=" + columnItems +
                    ", owners=" + owners +
                    '}';
        }
    }

    public static class GroupByItem {
        private SQLSelectGroupByClause expr;

        public GroupByItem(SQLSelectGroupByClause expr) {
            this.expr = expr;
        }

        public SQLSelectGroupByClause getExpr() {
            return expr;
        }

        public void setExpr(SQLSelectGroupByClause expr) {
            this.expr = expr;
        }

        @Override
        public String toString() {
            return "GroupByItem{" +
                    "expr=" + expr +
                    '}';
        }
    }
}
