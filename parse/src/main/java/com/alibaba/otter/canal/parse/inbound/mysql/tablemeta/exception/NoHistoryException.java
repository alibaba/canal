package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.exception;

public class NoHistoryException extends Exception{

    private String dbName;
    private String tbName;

    public NoHistoryException(String dbName, String tbName) {
        this.dbName = dbName;
        this.tbName = tbName;
    }

    public void printTableName() {
        System.out.println(dbName+"."+tbName);
    }

    @Override
    public String toString() {
        return "NioHistoryException: " + dbName + " " + tbName;
    }
}
