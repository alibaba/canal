package com.alibaba.otter.canal.store.file.model;

import java.nio.ByteBuffer;

import com.alibaba.otter.canal.store.file.maped.MapedFile;

public class SelectMapedBufferResult {

    // 从队列中哪个绝对Offset开始
    private final long       startOffset;
    // position从0开始
    private final ByteBuffer byteBuffer;
    // 有效数据大小
    private int              size;
    // 用来释放内存
    private MapedFile        mapedFile;

    public SelectMapedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MapedFile mapedFile){
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mapedFile = mapedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public MapedFile getMapedFile() {
        return mapedFile;
    }

    /**
     * 此方法只能被调用一次，重复调用无效
     */
    public synchronized void release() {
        if (this.mapedFile != null) {
            this.mapedFile.release();
            this.mapedFile = null;
        }
    }

    @Override
    protected void finalize() {
        if (this.mapedFile != null) {
            this.release();
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
