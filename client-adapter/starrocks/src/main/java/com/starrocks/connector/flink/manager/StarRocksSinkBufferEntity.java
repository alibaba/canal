/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.manager;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

public class StarRocksSinkBufferEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    private ArrayList<byte[]> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private String label;
    private String database;
    private String table;
    private boolean EOF;
    private String labelPrefix;

    public StarRocksSinkBufferEntity(String database, String table, String labelPrefix) {
        this.database = database;
        this.table = table;
        this.labelPrefix = labelPrefix;
        label = createBatchLabel();
    }

    public StarRocksSinkBufferEntity asEOF() {
        EOF = true;
        return this;
    }

    public boolean EOF() {
        return EOF;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getLabel() {
        return label;
    }

    public void setBuffer(ArrayList<byte[]> buffer) {
        this.buffer = buffer;
    }

    public ArrayList<byte[]> getBuffer() {
        return buffer;
    }

    public void addToBuffer(byte[] bts) {
        incBatchCount();
        incBatchSize(bts.length);
        buffer.add(bts);
    }
    public int getBatchCount() {
        return batchCount;
    }
    private void incBatchCount() {
        this.batchCount += 1;
    }
    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void incBatchSize(long batchSize) {
        this.batchSize += batchSize;
    }

    public synchronized void clear() {
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
        label = createBatchLabel();
    }

    public String reGenerateLabel() {
        return label = createBatchLabel();
    }

    public String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(labelPrefix)) {
            sb.append(labelPrefix);
        }
        return sb.append(UUID.randomUUID().toString()).toString();
    }
}
