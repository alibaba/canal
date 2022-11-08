package com.alibaba.otter.canal.admin.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Pager<T> implements Serializable {

    private static final long serialVersionUID = -986577815091763517L;

    private Long              count            = 0L;
    private List<T>           items            = new ArrayList<>();
    private Integer           page             = 1;
    private Integer           size             = 20;
    private Long              offset           = 0L;

    public Pager(){

    }

    public Pager(Integer page, Integer size){
        this.page = page;
        this.size = size;
    }

    public Pager(Long count, List<T> items){
        this.count = count;
        this.items = items;
    }

    public String toString() {
        return "PageResult[count=" + this.count + ", items=" + this.items + "]";
    }

    public Long getCount() {
        return this.count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public Integer getPage() {
        if (page == null) {
            page = 1;
        }
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getSize() {
        if (size == null) {
            size = 20;
        }
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Long getOffset() {
        offset = (long) (getPage() - 1) * (long) getSize();
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }
}
