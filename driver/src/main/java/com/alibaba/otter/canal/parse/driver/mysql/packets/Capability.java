package com.alibaba.otter.canal.parse.driver.mysql.packets;

/**
 * https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol
 * ::CapabilityFlags
 */
public interface Capability {

    // Use the improved version of Old Password Authentication.
    // Assumed to be set since 4.1.1.
    int CLIENT_LONG_PASSWORD                  = 0x00000001;

    // Send found rows instead of affected rows in EOF_Packet.
    int CLIENT_FOUND_ROWS                     = 0x00000002;

    // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition320
    // Longer flags in Protocol::ColumnDefinition320.
    // Server:Supports longer flags.
    // Client:Expects longer flags.
    // 执行查询sql时，除了返回结果集，还返回元数据
    int CLIENT_LONG_FLAG                      = 0x00000004;

    // 可以在handshake时，指定一个数据库名
    // Database (schema) name can be specified on connect in Handshake Response
    // Packet.
    // Server: Supports schema-name in Handshake Response Packet.
    // Client: Handshake Response Packet contains a schema-name.
    int CLIENT_CONNECT_WITH_DB                = 0x00000008;

    // Server: Do not permit database.table.column.
    int CLIENT_NO_SCHEMA                      = 0x00000010;

    // Compression protocol supported.
    // Server:Supports compression.
    // Client:Switches to Compression compressed protocol after successful
    // authentication.
    int CLIENT_COMPRESS                       = 0x00000020;

    // Special handling of ODBC behavior.
    // No special behavior since 3.22.
    int CLIENT_ODBC                           = 0x00000040;

    // Can use LOAD DATA LOCAL.
    // Server:Enables the LOCAL INFILE request of LOAD DATA|XML.
    // Client:Will handle LOCAL INFILE request.
    int CLIENT_LOCAL_FILES                    = 0x00000080;

    // Server: Parser can ignore spaces before '('.
    // Client: Let the parser ignore spaces before '('.
    int CLIENT_IGNORE_SPACE                   = 0x00000100;

    // Server:Supports the 4.1 protocol,
    // 4.1协议中，
    // OKPacket将会包含warning count
    // ERR_Packet包含SQL state
    // EOF_Packet包含warning count和status flags
    // Client:Uses the 4.1 protocol.
    // Note: this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0
    // If CLIENT_PROTOCOL_41 is set：
    // 1、the ok packet contains a warning count.
    // https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    // 2、ERR_Packet It contains a SQL state value if CLIENT_PROTOCOL_41 is
    // enabled. //https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
    // 3、EOF_Packet If CLIENT_PROTOCOL_41 is enabled, the EOF packet contains a
    // warning count and status flags.
    // https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
    int CLIENT_PROTOCOL_41                    = 0x00000200;

    // wait_timeout versus wait_interactive_timeout.
    // Server:Supports interactive and noninteractive clients.
    // Client:Client is interactive.
    int CLIENT_INTERACTIVE                    = 0x00000400;

    // Server: Supports SSL.
    // Client: Switch to SSL after sending the capability-flags.
    int CLIENT_SSL                            = 0x00000800;

    // Client: Do not issue SIGPIPE if network failures occur (libmysqlclient
    // only).
    int CLIENT_IGNORE_SIGPIPE                 = 0x00001000;

    // Server: Can send status flags in EOF_Packet.
    // Client:Expects status flags in EOF_Packet.
    // Note:This flag is optional in 3.23, but always set by the server since
    // 4.0.
    int CLIENT_TRANSACTIONS                   = 0x00002000;

    // Unused
    // Note: Was named CLIENT_PROTOCOL_41 in 4.1.0.
    int CLIENT_RESERVED                       = 0x00004000;

    /**
     * <pre>
     *      服务端返回20 byte随机字节，客户端利用其对密码进行加密，加密算法如下：
     *      https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41
     *      Authentication::Native41:
     *      client-side expects a 20-byte random challenge
     *      client-side returns a 20-byte response based on the algorithm described later
     *      Name
     *      mysql_native_password
     *      Requires
     *      CLIENT_SECURE_CONNECTION
     *      Image description follows.
     *      Image description
     *      This method fixes a 2 short-comings of the Old Password Authentication:
     *      (https://dev.mysql.com/doc/internals/en/old-password-authentication.html#packet-Authentication::Old)
     *      using a tested, crypto-graphic hashing function which isn't broken
     *      knowning the content of the hash in the mysql.user table isn't enough to authenticate against the MySQL Server.
     *      The password is calculated by:
     *      SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
     * </pre>
     */
    int CLIENT_SECURE_CONNECTION              = 0x00008000;

    // Server:Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
    // Client:May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
    // Note:Was named CLIENT_MULTI_QUERIES in 4.1.0, renamed later.
    // Requires:CLIENT_PROTOCOL_41
    int CLIENT_MULTI_STATEMENTS               = 0x00010000;

    // Server: Can send multiple resultsets for COM_QUERY.
    // Client: Can handle multiple resultsets for COM_QUERY.
    // Requires:CLIENT_PROTOCOL_41
    int CLIENT_MULTI_RESULTS                  = 0x00020000;

    // Server: Can send multiple resultsets for ComStmtExecutePacket.
    // Client: Can handle multiple resultsets for ComStmtExecutePacket.
    // Requires:CLIENT_PROTOCOL_41
    int CLIENT_PS_MULTI_RESULTS               = 0x00040000;

    // Server:Sends extra data in Initial Handshake Packet and supports the
    // pluggable authentication protocol.
    // Client: Supports authentication plugins.
    // Requires: CLIENT_PROTOCOL_41
    int CLIENT_PLUGIN_AUTH                    = 0x00080000;

    // Server: Permits connection attributes in Protocol::HandshakeResponse41.
    // Client: Sends connection attributes in Protocol::HandshakeResponse41.
    int CLIENT_CONNECT_ATTRS                  = 0x00100000;

    // Server:Understands length-encoded integer for auth response data in
    // Protocol::HandshakeResponse41.
    // Client:Length of auth response data in Protocol::HandshakeResponse41 is a
    // length-encoded integer.
    // Note: The flag was introduced in 5.6.6, but had the wrong value.
    int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    // Server: Announces support for expired password extension.
    // Client: Can handle expired passwords.
    int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x00400000;

    // Server: Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send
    // session-state change data after a OK packet.
    // Client: Expects the server to send sesson-state changes after a OK
    // packet.
    int CLIENT_SESSION_TRACK                  = 0x00800000;

    /**
     * Server: Can send OK after a Text Resultset. Client: Expects an OK
     * (instead of EOF) after the resultset rows of a Text Resultset.
     * Background:To support CLIENT_SESSION_TRACK, additional information must
     * be sent after all successful commands. Although the OK packet is
     * extensible, the EOF packet is not due to the overlap of its bytes with
     * the content of the Text Resultset Row. Therefore, the EOF packet in the
     * Text Resultset is replaced with an OK packet. EOF packets are deprecated
     * as of MySQL 5.7.5.
     */
    int CLIENT_DEPRECATE_EOF                  = 0x01000000;

}
