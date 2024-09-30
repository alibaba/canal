package com.alibaba.otter.canal.parse.driver.mysql.ssl;

public enum SslMode {

    /**
     * 关闭SSL
     */
    DISABLED,
    /**
     * 尝试SSL传输
     */
    PREFERRED,
    /**
     * 要求SSL传输，不校验证书
     */
    REQUIRED,
    /**
     * 要求SSL传输，校验证书，不校验证书里的域名
     */
    VERIFY_CA,
    /**
     * 要求SSL传输，校验证书，校验证书里的域名
     */
    VERIFY_IDENTITY

}
