package com.alibaba.otter.canal.parse.driver.mysql.ssl;

/**
 * @author 枭博
 * @date 2024/05/14
 */
public class SslInfo {

    private SslMode sslMode = SslMode.DISABLED;
    private String tlsVersions; // 和 enabledTLSProtocols 同含义，TLSv1.2,TLSv1.3
    private String trustCertificateKeyStoreType; // trustStore 证书类型，支持 JKS (默认) 和 PKCS12
    private String trustCertificateKeyStorePath; // trustStore 证书路径
    private String trustCertificateKeyStorePassword; // trustStore 证书密码
    private String clientCertificateKeyStoreType; // client 证书类型，支持 JKS (默认) 和 PKCS12
    private String clientCertificateKeyStorePath; // client 证书路径
    private String clientCertificateKeyStorePassword; // client 证书密码

    public SslInfo(SslMode sslMode, String tlsVersions, String trustCertificateKeyStoreType,
        String trustCertificateKeyStorePath, String trustCertificateKeyStorePassword,
        String clientCertificateKeyStoreType,
        String clientCertificateKeyStorePath, String clientCertificateKeyStorePassword) {
        this.sslMode = sslMode;
        this.tlsVersions = tlsVersions;
        this.trustCertificateKeyStoreType = trustCertificateKeyStoreType;
        this.trustCertificateKeyStorePath = trustCertificateKeyStorePath;
        this.trustCertificateKeyStorePassword = trustCertificateKeyStorePassword;
        this.clientCertificateKeyStoreType = clientCertificateKeyStoreType;
        this.clientCertificateKeyStorePath = clientCertificateKeyStorePath;
        this.clientCertificateKeyStorePassword = clientCertificateKeyStorePassword;
    }

    public SslInfo() {
    }

    public SslMode getSslMode() {
        return sslMode;
    }

    public void setSslMode(SslMode sslMode) {
        this.sslMode = sslMode;
    }

    public String getTlsVersions() {
        return tlsVersions;
    }

    public void setTlsVersions(String tlsVersions) {
        this.tlsVersions = tlsVersions;
    }

    public String getTrustCertificateKeyStoreType() {
        return trustCertificateKeyStoreType;
    }

    public void setTrustCertificateKeyStoreType(String trustCertificateKeyStoreType) {
        this.trustCertificateKeyStoreType = trustCertificateKeyStoreType;
    }

    public String getTrustCertificateKeyStorePath() {
        return trustCertificateKeyStorePath;
    }

    public void setTrustCertificateKeyStorePath(String trustCertificateKeyStorePath) {
        this.trustCertificateKeyStorePath = trustCertificateKeyStorePath;
    }

    public String getTrustCertificateKeyStorePassword() {
        return trustCertificateKeyStorePassword;
    }

    public void setTrustCertificateKeyStorePassword(String trustCertificateKeyStorePassword) {
        this.trustCertificateKeyStorePassword = trustCertificateKeyStorePassword;
    }

    public String getClientCertificateKeyStoreType() {
        return clientCertificateKeyStoreType;
    }

    public void setClientCertificateKeyStoreType(String clientCertificateKeyStoreType) {
        this.clientCertificateKeyStoreType = clientCertificateKeyStoreType;
    }

    public String getClientCertificateKeyStorePath() {
        return clientCertificateKeyStorePath;
    }

    public void setClientCertificateKeyStorePath(String clientCertificateKeyStorePath) {
        this.clientCertificateKeyStorePath = clientCertificateKeyStorePath;
    }

    public String getClientCertificateKeyStorePassword() {
        return clientCertificateKeyStorePassword;
    }

    public void setClientCertificateKeyStorePassword(String clientCertificateKeyStorePassword) {
        this.clientCertificateKeyStorePassword = clientCertificateKeyStorePassword;
    }
}
