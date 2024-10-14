package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.cert.*;
import java.util.*;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslInfo;
import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslMode;

/**
 * @author luoyaogui 实现channel的管理（监听连接、读数据、回收） 2016-12-28
 * @author chuanyi 2018-3-3 保留<code>open</code>减少文件变更数量
 */
public abstract class BioSocketChannelPool {

    private static final Logger logger = LoggerFactory.getLogger(BioSocketChannelPool.class);

    public static BioSocketChannel open(SocketAddress address) throws Exception {
        Socket socket = createSocket(address);
        return new BioSocketChannel(socket);
    }

    public static BioSocketChannel openSsl(Socket socket, SslInfo sslInfo) throws Exception {
        SslMode sslMode = sslInfo.getSslMode();
        switch (sslMode) {
            case REQUIRED:
            case PREFERRED:
            case VERIFY_CA:
            case VERIFY_IDENTITY:
                SSLSocket sslSocket = createSslSocket(socket, sslInfo);
                return new BioSocketChannel(sslSocket);
            default:
                throw new UnsupportedOperationException("Unsupported ssl mode: " + sslMode);
        }
    }

    private static Socket createSocket(SocketAddress address) throws IOException {
        Socket socket;
        socket = new Socket();
        socket.setSoTimeout(BioSocketChannel.SO_TIMEOUT);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setReuseAddress(true);
        socket.connect(address, BioSocketChannel.DEFAULT_CONNECT_TIMEOUT);
        return socket;
    }

    /**
     * from JDBC driver com.mysql.cj.protocol.ExportControlled#performTlsHandshake
     * com.mysql.cj.protocol.ExportControlled#getSSLContext
     *
     * @param socket
     * @param sslInfo
     * @return
     * @throws Exception
     */
    private static SSLSocket createSslSocket(Socket socket, SslInfo sslInfo) throws Exception {
        SslMode sslMode = sslInfo.getSslMode();
        boolean verifyServerCert = sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;

        String clientCertificateKeyStoreUrl = sslInfo.getClientCertificateKeyStoreUrl();
        String clientCertificateKeyStoreType = sslInfo.getClientCertificateKeyStoreType() != null ? sslInfo
            .getClientCertificateKeyStoreType() : "JKS";
        String clientCertificateKeyStorePassword = sslInfo.getClientCertificateKeyStorePassword();
        String trustCertificateKeyStoreUrl = sslInfo.getTrustCertificateKeyStoreUrl();
        String trustCertificateKeyStoreType = sslInfo.getTrustCertificateKeyStoreType() != null ? sslInfo
            .getTrustCertificateKeyStoreType() : "JKS";
        String trustCertificateKeyStorePassword = sslInfo.getTrustCertificateKeyStorePassword();
        boolean fallbackToDefaultTrustStore = true;
        String hostName = sslMode == SslMode.VERIFY_IDENTITY ? socket.getInetAddress().getHostName() : null;

        SSLContext sslContext = getSSLContext(clientCertificateKeyStoreUrl,
            clientCertificateKeyStoreType,
            clientCertificateKeyStorePassword,
            trustCertificateKeyStoreUrl,
            trustCertificateKeyStoreType,
            trustCertificateKeyStorePassword,
            fallbackToDefaultTrustStore,
            verifyServerCert,
            hostName);
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();

        SSLSocket sslSocket = (SSLSocket) socketFactory
            .createSocket(socket, socket.getInetAddress().getHostName(), socket.getPort(), true);

        String[] protocolArr = null;
        if (StringUtils.isNotEmpty(sslInfo.getTlsVersions())) {
            protocolArr = StringUtils.split(sslInfo.getTlsVersions(), ",");
        }
        if (protocolArr == null || protocolArr.length == 0) {
            protocolArr = new String[] { "TLSv1.2", "TLSv1.3" };
        }
        logger.info("SSL protocol: {}", StringUtils.join(protocolArr, ","));
        sslSocket.setEnabledProtocols(protocolArr);
        sslSocket.setSoTimeout(BioSocketChannel.SO_TIMEOUT);
        sslSocket.startHandshake();
        logger.info("SSL socket handshake success.");
        return sslSocket;
    }

    private static SSLContext getSSLContext(String clientCertificateKeyStoreUrl, String clientCertificateKeyStoreType,
                                            String clientCertificateKeyStorePassword,
                                            String trustCertificateKeyStoreUrl, String trustCertificateKeyStoreType,
                                            String trustCertificateKeyStorePassword,
                                            boolean fallbackToDefaultTrustStore, boolean verifyServerCert,
                                            String hostName) throws Exception {

        KeyManager[] kms = null;
        List<TrustManager> tms = new ArrayList<>();

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        if (StringUtils.isNotEmpty(clientCertificateKeyStoreUrl)) {
            InputStream ksIS = null;
            try {
                if (StringUtils.isNotEmpty(clientCertificateKeyStoreType)) {
                    KeyStore clientKeyStore = KeyStore.getInstance(clientCertificateKeyStoreType);
                    URL ksURL = new URL(clientCertificateKeyStoreUrl);
                    char[] password = (clientCertificateKeyStorePassword == null) ? new char[0] : clientCertificateKeyStorePassword
                        .toCharArray();
                    ksIS = ksURL.openStream();
                    clientKeyStore.load(ksIS, password);
                    kmf.init(clientKeyStore, password);
                    kms = kmf.getKeyManagers();
                }
            } finally {
                if (ksIS != null) {
                    try {
                        ksIS.close();
                    } catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized so we
                        // shouldn't throw this exception
                    }
                }
            }
        }

        InputStream trustStoreIS = null;
        try {
            String trustStoreType = "";
            char[] trustStorePassword = null;
            KeyStore trustKeyStore = null;

            if (StringUtils.isNotEmpty(trustCertificateKeyStoreUrl)
                && StringUtils.isNotEmpty(trustCertificateKeyStoreType)) {
                trustStoreType = trustCertificateKeyStoreType;
                trustStorePassword = (trustCertificateKeyStorePassword == null) ? new char[0] : trustCertificateKeyStorePassword
                    .toCharArray();
                trustStoreIS = new URL(trustCertificateKeyStoreUrl).openStream();

                trustKeyStore = KeyStore.getInstance(trustStoreType);
                trustKeyStore.load(trustStoreIS, trustStorePassword);
            }

            if (trustKeyStore != null || fallbackToDefaultTrustStore) {
                tmf.init(trustKeyStore);
                // (trustKeyStore == null) initializes the TrustManagerFactory with the default
                // truststore.

                // building the customized list of TrustManagers from original one if it's
                // available
                TrustManager[] origTms = tmf.getTrustManagers();

                for (TrustManager tm : origTms) {
                    // wrap X509TrustManager or put original if non-X509 TrustManager
                    tms.add(tm instanceof X509TrustManager ? new X509TrustManagerWrapper((X509TrustManager) tm,
                        verifyServerCert,
                        hostName) : tm);
                }
            }

        } finally {
            if (trustStoreIS != null) {
                try {
                    trustStoreIS.close();
                } catch (IOException e) {
                    // can't close input stream, but keystore can be properly initialized so we
                    // shouldn't throw this exception
                }
            }
        }

        // if original TrustManagers are not available then putting one
        // X509TrustManagerWrapper which take care only
        // about expiration check
        if (tms.size() == 0) {
            tms.add(new X509TrustManagerWrapper(verifyServerCert, hostName));
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kms, tms.toArray(new TrustManager[tms.size()]), null);
        return sslContext;
    }

    public static class X509TrustManagerWrapper implements X509TrustManager {

        private X509TrustManager   origTm           = null;
        private boolean            verifyServerCert = false;
        private String             hostName         = null;
        private CertificateFactory certFactory      = null;
        private PKIXParameters     validatorParams  = null;
        private CertPathValidator  validator        = null;

        public X509TrustManagerWrapper(X509TrustManager tm, boolean verifyServerCertificate,
                                       String hostName) throws CertificateException{
            this.origTm = tm;
            this.verifyServerCert = verifyServerCertificate;
            this.hostName = hostName;

            if (verifyServerCertificate) {
                try {
                    Set<TrustAnchor> anch = Arrays.stream(tm.getAcceptedIssuers())
                        .map(c -> new TrustAnchor(c, null))
                        .collect(Collectors.toSet());
                    this.validatorParams = new PKIXParameters(anch);
                    this.validatorParams.setRevocationEnabled(false);
                    this.validator = CertPathValidator.getInstance("PKIX");
                    this.certFactory = CertificateFactory.getInstance("X.509");
                } catch (Exception e) {
                    throw new CertificateException(e);
                }
            }

        }

        public X509TrustManagerWrapper(boolean verifyServerCertificate, String hostName){
            this.verifyServerCert = verifyServerCertificate;
            this.hostName = hostName;
        }

        public X509Certificate[] getAcceptedIssuers() {
            return this.origTm != null ? this.origTm.getAcceptedIssuers() : new X509Certificate[0];
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            for (int i = 0; i < chain.length; i++) {
                chain[i].checkValidity();
            }

            if (this.validatorParams != null) {
                X509CertSelector certSelect = new X509CertSelector();
                certSelect.setSerialNumber(chain[0].getSerialNumber());

                try {
                    CertPath certPath = this.certFactory.generateCertPath(Arrays.asList(chain));
                    // Validate against truststore
                    CertPathValidatorResult result = this.validator.validate(certPath, this.validatorParams);
                    // Check expiration for the CA used to validate this path
                    ((PKIXCertPathValidatorResult) result).getTrustAnchor().getTrustedCert().checkValidity();

                } catch (InvalidAlgorithmParameterException e) {
                    throw new CertificateException(e);
                } catch (CertPathValidatorException e) {
                    throw new CertificateException(e);
                }
            }

            if (this.verifyServerCert) {
                if (this.origTm != null) {
                    this.origTm.checkServerTrusted(chain, authType);
                } else {
                    throw new CertificateException(
                        "Can't verify server certificate because no trust manager is found.");
                }

                // verify server certificate identity
                if (this.hostName != null) {
                    logger.info("verify hostName: {}", this.hostName);
                    Set<String> expectHostNames = new HashSet<>();
                    for (X509Certificate certificate : chain) {
                        String dn = certificate.getSubjectX500Principal().getName(X500Principal.RFC2253);
                        String cn = null;
                        try {
                            LdapName ldapDN = new LdapName(dn);
                            for (Rdn rdn : ldapDN.getRdns()) {
                                if (rdn.getType().equalsIgnoreCase("CN")) {
                                    cn = rdn.getValue().toString();
                                    break;
                                }
                            }
                        } catch (InvalidNameException e) {
                            throw new CertificateException(
                                "Failed to retrieve the Common Name (CN) from the server certificate.");
                        }
                        expectHostNames.add(cn);
                    }

                    if (!expectHostNames.contains(this.hostName)) {
                        throw new CertificateException(
                            "Server certificate identity check failed. The certificate Common Name "
                                                       + expectHostNames.stream()
                                                           .map(h -> "'" + h + "'")
                                                           .collect(Collectors.joining(", "))
                                                       + " does not match with '" + this.hostName + "'.");
                    }

                }
            }
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.origTm.checkClientTrusted(chain, authType);
        }
    }

}
