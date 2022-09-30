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

package com.starrocks.connector.flink.connection;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * JDBC connection options.
 */
@PublicEvolving
public class StarRocksJdbcConnectionOptions  implements Serializable {
    
    private static final long serialVersionUID = 1L;

    protected final String url;
    protected final String driverName;
    protected final String cjDriverName;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;

    public StarRocksJdbcConnectionOptions(String url, String username, String password) {
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.driverName = "com.mysql.jdbc.Driver";
        this.cjDriverName = "com.mysql.cj.jdbc.Driver";
        this.username = username;
        this.password = password;
    }

    public String getDbURL() {
        return url;
    }

    public String getCjDriverName() {
        return cjDriverName;
    }

    public String getDriverName() {
        return driverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
