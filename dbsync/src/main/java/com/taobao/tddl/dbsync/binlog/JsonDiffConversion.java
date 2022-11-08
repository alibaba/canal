package com.taobao.tddl.dbsync.binlog;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.dbsync.binlog.JsonConversion.Json_Value;
import com.taobao.tddl.dbsync.binlog.JsonConversion.Json_enum_type;

/**
 * 处理mysql8.0 parital json diff解析
 * 
 * @author agapple 2018年11月4日 下午3:53:46
 * @since 1.1.2
 */
public class JsonDiffConversion {

    /**
     * The JSON value in the given path is replaced with a new value. It has the
     * same effect as `JSON_REPLACE(col, path, value)`.
     */
    public static final int DIFF_OPERATION_REPLACE    = 0;
    /**
     * Add a new element at the given path. If the path specifies an array
     * element, it has the same effect as `JSON_ARRAY_INSERT(col, path, value)`.
     * If the path specifies an object member, it has the same effect as
     * `JSON_INSERT(col, path, value)`.
     */
    public static final int DIFF_OPERATION_INSERT     = 1;
    /**
     * The JSON value at the given path is removed from an array or object. It
     * has the same effect as `JSON_REMOVE(col, path)`.
     */
    public static final int DIFF_OPERATION_REMOVE     = 2;

    public static final int JSON_DIFF_OPERATION_COUNT = 3;

    public static StringBuilder print_json_diff(LogBuffer buffer, long len, String columnName, int columnIndex,
                                                String charsetName) {
        int position = buffer.position();
        List<String> operation_names = new ArrayList<>();
        while (buffer.hasRemaining()) {
            int operation_int = buffer.getUint8();
            if (operation_int >= JSON_DIFF_OPERATION_COUNT) {
                throw new IllegalArgumentException("reading operation type (invalid operation code)");
            }

            // skip path
            long path_length = buffer.getPackedLong();
            if (path_length > len) {
                throw new IllegalArgumentException("skipping path");
            }

            // compute operation name
            byte[] lastP = buffer.getData(buffer.position() + (int) path_length - 1, 1);
            String operation_name = json_diff_operation_name(operation_int, lastP[0]);
            operation_names.add(operation_name);

            buffer.forward((int) path_length);
            // skip value
            if (operation_int != DIFF_OPERATION_REMOVE) {
                long value_length = buffer.getPackedLong();
                if (value_length > len) {
                    throw new IllegalArgumentException("skipping path");
                }

                buffer.forward((int) value_length);
            }
        }

        // Print function names in reverse order.
        StringBuilder builder = new StringBuilder();
        for (int i = operation_names.size() - 1; i >= 0; i--) {
            if (i == 0 || operation_names.get(i - 1) != operation_names.get(i)) {
                builder.append(operation_names.get(i)).append("(");
            }
        }

        // Print column id
        if (columnName != null) {
            builder.append(columnName);
        } else {
            builder.append("@").append(columnIndex);
        }

        // In case this vector is empty (a no-op), make an early return
        // after printing only the column name
        if (operation_names.size() == 0) {
            return builder;
        }

        // Print comma between column name and next function argument
        builder.append(", ");
        // Print paths and values.
        buffer.position(position);
        int diff_i = 0;
        while (buffer.hasRemaining()) {
            // Read operation
            int operation_int = buffer.getUint8();

            // Read path length
            long path_length = buffer.getPackedLong();
            // Print path
            builder.append('\'').append(buffer.getFixString((int) path_length)).append('\'');

            if (operation_int != DIFF_OPERATION_REMOVE) {
                // Print comma between path and value
                builder.append(", ");
                // Read value length
                long value_length = buffer.getPackedLong();

                Json_Value jsonValue = JsonConversion.parse_value(buffer.getUint8(),
                    buffer,
                    value_length - 1,
                    charsetName);
                buffer.forward((int) value_length - 1);
                // Read value
                if (jsonValue.m_type == Json_enum_type.ERROR) {
                    throw new IllegalArgumentException("parsing json value");
                }
                StringBuilder jsonBuilder = new StringBuilder();
                jsonValue.toJsonString(jsonBuilder, charsetName);
                builder.append(jsonBuilder);
            }

            // Print closing parenthesis
            if (!buffer.hasRemaining() || operation_names.get(diff_i + 1) != operation_names.get(diff_i)) {
                builder.append(")");
            }

            if (buffer.hasRemaining()) {
                builder.append(", ");
            }
            diff_i++;
        }

        return builder;
    }

    private static String json_diff_operation_name(int operationType, int last_path_char) {
        switch (operationType) {
            case DIFF_OPERATION_REPLACE:
                return "JSON_REPLACE";
            case DIFF_OPERATION_INSERT:
                if (last_path_char == ']') {
                    return "JSON_ARRAY_INSERT";
                } else {
                    return "JSON_INSERT";
                }
            case DIFF_OPERATION_REMOVE:
                return "JSON_REMOVE";
        }
        throw new IllegalArgumentException("illeagal operationType : " + operationType);
    }
}
