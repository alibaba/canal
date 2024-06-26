package com.alibaba.otter.canal.parse.driver.mysql.ssl;

/**
 * @author 枭博
 * @date 2024/05/14
 */
public class SslInfo {

    private SslMode sslMode = SslMode.DISABLED;
    private String  tlsVersions;                       // 和 enabledTLSProtocols 同含义，TLSv1.2,TLSv1.3
    private String  trustCertificateKeyStoreType;      // trustStore 证书类型，支持 JKS (默认) 和 PKCS12
    private String  trustCertificateKeyStoreUrl;       // trustStore 证书
    private String  trustCertificateKeyStorePassword;  // trustStore 证书密码
    private String  clientCertificateKeyStoreType;     // client 证书类型，支持 JKS (默认) 和 PKCS12
    private String  clientCertificateKeyStoreUrl;      // client 证书
    private String  clientCertificateKeyStorePassword; // client 证书密码

    public SslInfo(SslMode sslMode, String tlsVersions, String trustCertificateKeyStoreType,
                   String trustCertificateKeyStoreUrl, String trustCertificateKeyStorePassword,
                   String clientCertificateKeyStoreType, String clientCertificateKeyStoreUrl,
                   String clientCertificateKeyStorePassword) {
        this.sslMode = sslMode;
        this.tlsVersions = tlsVersions;
        this.trustCertificateKeyStoreType = trustCertificateKeyStoreType;
        this.trustCertificateKeyStoreUrl = trustCertificateKeyStoreUrl;
        this.trustCertificateKeyStorePassword = trustCertificateKeyStorePassword;
        this.clientCertificateKeyStoreType = clientCertificateKeyStoreType;
        this.clientCertificateKeyStoreUrl = clientCertificateKeyStoreUrl;
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

    public String getTrustCertificateKeyStoreUrl() {
        return trustCertificateKeyStoreUrl;
    }

    public void setTrustCertificateKeyStoreUrl(String trustCertificateKeyStoreUrl) {
        this.trustCertificateKeyStoreUrl = trustCertificateKeyStoreUrl;
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

    public String getClientCertificateKeyStoreUrl() {
        return clientCertificateKeyStoreUrl;
    }

    public void setClientCertificateKeyStoreUrl(String clientCertificateKeyStoreUrl) {
        this.clientCertificateKeyStoreUrl = clientCertificateKeyStoreUrl;
    }

    public String getClientCertificateKeyStorePassword() {
        return clientCertificateKeyStorePassword;
    }

    public void setClientCertificateKeyStorePassword(String clientCertificateKeyStorePassword) {
        this.clientCertificateKeyStorePassword = clientCertificateKeyStorePassword;
    }
}
