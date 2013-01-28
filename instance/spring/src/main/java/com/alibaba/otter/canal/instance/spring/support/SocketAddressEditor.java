package com.alibaba.otter.canal.instance.spring.support;

import java.beans.PropertyEditorSupport;
import java.net.InetSocketAddress;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.PropertyEditorRegistrar;
import org.springframework.beans.PropertyEditorRegistry;

public class SocketAddressEditor extends PropertyEditorSupport implements PropertyEditorRegistrar {

    public void registerCustomEditors(PropertyEditorRegistry registry) {
        registry.registerCustomEditor(InetSocketAddress.class, this);
    }

    public void setAsText(String text) throws IllegalArgumentException {
        String[] addresses = StringUtils.split(text, ":");
        if (addresses.length > 0) {
            setValue(new InetSocketAddress(addresses[0], Integer.valueOf(addresses[1])));
        } else {
            setValue(null);
        }
    }

}
