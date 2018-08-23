package com.alibaba.otter.canal.client.adapter.hbase.support;

import java.util.ArrayList;
import java.util.List;

/**
 * HBase操作对象类
 *
 * @author machengyuan 2018-8-21 下午10:12:34
 * @version 1.0.0
 */
public class HRow {

    private byte[]      rowKey;
    private List<HCell> cells = new ArrayList<>();

    public HRow(){
    }

    public HRow(byte[] rowKey){
        this.rowKey = rowKey;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public List<HCell> getCells() {
        return cells;
    }

    public void setCells(List<HCell> cells) {
        this.cells = cells;
    }

    public void addCell(String family, String qualifier, byte[] value) {
        HCell hCell = new HCell(family, qualifier, value);
        cells.add(hCell);
    }

    public static class HCell {

        private String family;
        private String qualifier;
        private byte[] value;

        public HCell(){
        }

        public HCell(String family, String qualifier, byte[] value){
            this.family = family;
            this.qualifier = qualifier;
            this.value = value;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public String getQualifier() {
            return qualifier;
        }

        public void setQualifier(String qualifier) {
            this.qualifier = qualifier;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }
    }
}
