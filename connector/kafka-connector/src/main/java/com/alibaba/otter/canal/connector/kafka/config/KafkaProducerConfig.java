package com.alibaba.otter.canal.connector.kafka.config;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.otter.canal.connector.core.config.MQProperties;

public class KafkaProducerConfig extends MQProperties {

    private Map<String, Object> kafkaProperties = new LinkedHashMap<>();

    private boolean             kerberosEnabled         = false;
    private String              krb5File;
    private String              jaasFile;

    public Map<String, Object> getKafkaProperties() {
        return kafkaProperties;
    }

    public void setKafkaProperties(Map<String, Object> kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public boolean isKerberosEnabled() {
        return kerberosEnabled;
    }

    public void setKerberosEnabled(boolean kerberosEnabled) {
        this.kerberosEnabled = kerberosEnabled;
    }

    public String getKrb5File() {
        return krb5File;
    }

    public void setKrb5File(String krb5File) {
        this.krb5File = krb5File;
    }

    public String getJaasFile() {
        return jaasFile;
    }

    public void setJaasFile(String jaasFile) {
        this.jaasFile = jaasFile;
    }
}
