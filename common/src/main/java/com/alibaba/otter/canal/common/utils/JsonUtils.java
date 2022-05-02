package com.alibaba.otter.canal.common.utils;

import java.lang.reflect.Type;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONFactory;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.fastjson2.filter.PropertyFilter;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.writer.ObjectWriter;


/**
 * 字节处理相关工具类
 * 
 * @author jianghang
 */
public class JsonUtils {

    static {
        JSON.register(InetAddress.class, InetAddressWriter.instance);
        JSON.register(Inet4Address.class, InetAddressWriter.instance);
        JSON.register(Inet6Address.class, InetAddressWriter.instance);

        JSONFactory.getDefaultObjectReaderProvider().addAutoTypeAccept("com.alibaba.otter.");
        JSONFactory.getDefaultObjectReaderProvider().addAutoTypeAccept("com.taobao.tddl.dbsync.");
    }

    public static <T> T unmarshalFromByte(byte[] bytes, Class<T> targetClass) {
        return (T) JSON.parseObject(bytes, targetClass);// 默认为UTF-8
    }

    public static <T> T unmarshalFromByte(byte[] bytes, TypeReference<T> type) {
        return (T) JSON.parseObject(bytes, type.getType());
    }

    public static byte[] marshalToByte(Object obj) {
        return JSON.toJSONBytes(obj); // 默认为UTF-8
    }

    public static byte[] marshalToByte(Object obj, JSONWriter.Feature... features) {
        return JSON.toJSONBytes(obj, features); // 默认为UTF-8
    }

    public static <T> T unmarshalFromString(String json, Class<T> targetClass) {
        return (T) JSON.parseObject(json, targetClass);// 默认为UTF-8
    }

    public static <T> T unmarshalFromString(String json, TypeReference<T> type) {
        return (T) JSON.parseObject(json, type);// 默认为UTF-8
    }

    public static String marshalToString(Object obj) {
        return JSON.toJSONString(obj); // 默认为UTF-8
    }

    public static String marshalToString(Object obj, JSONWriter.Feature... features) {
        return JSON.toJSONString(obj, features); // 默认为UTF-8
    }

    /**
     * 可以允许指定一些过滤字段进行生成json对象
     */
    public static String marshalToString(Object obj, String... fliterFields) {
        final List<String> propertyFliters = Arrays.asList(fliterFields);

        return JSON.toJSONString(obj, new PropertyFilter() {
            @Override
            public boolean process(Object object, String name, Object value) {
                return !propertyFliters.contains(name);
            }
        });
    }

    public static class InetAddressWriter implements ObjectWriter {

        public static InetAddressWriter instance = new InetAddressWriter();

        @Override
        public void write(JSONWriter jsonWriter, Object object, Object fieldName, Type fieldType, long features) {
            if (object == null) {
                jsonWriter.writeNull();
                return;
            }

            InetAddress address = (InetAddress) object;
            // 优先使用name
            jsonWriter.writeString(address.getHostName());
        }
    }
}
