/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
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

package com.alibaba.otter.canal.example.db.dialect;

/**
 * sql构造模板操作
 * 
 * @author jianghang 2011-10-27 下午01:31:15
 * @version 4.0.0
 */
public interface SqlTemplate {

    public String getSelectSql(String schemaName, String tableName, String[] pkNames, String[] columnNames);

    public String getUpdateSql(String schemaName, String tableName, String[] pkNames, String[] columnNames);

    public String getDeleteSql(String schemaName, String tableName, String[] pkNames);

    public String getInsertSql(String schemaName, String tableName, String[] pkNames, String[] columnNames);

    /**
     * 获取对应的mergeSql
     */
    public String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] columnNames,
                              String[] viewColumnNames, boolean updatePks);
}
