package com.alibaba.otter.canal.connector.rabbitmq.producer;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import com.alibaba.mq.amqp.utils.UserUtils;
import com.rabbitmq.client.impl.CredentialsProvider;

/**
 * aliyun amqp协议 账号类 暂不支持STS授权情况
 *
 * @version 1.0.0
 */
public class AliyunCredentialsProvider implements CredentialsProvider {

    /**
     * Access Key ID
     */
    private final String AliyunAccessKey;

    /**
     * Access Key Secret
     */
    private final String AliyunAccessSecret;

    /**
     * 资源主账号ID
     */
    private final long   resourceOwnerId;

    public AliyunCredentialsProvider(final String accessKey, final String accessSecret, final long resourceOwnerId){
        this.AliyunAccessKey = accessKey;
        this.AliyunAccessSecret = accessSecret;
        this.resourceOwnerId = resourceOwnerId;
    }

    @Override
    public String getUsername() {
        return UserUtils.getUserName(AliyunAccessKey, resourceOwnerId);
    }

    @Override
    public String getPassword() {
        try {
            return UserUtils.getPassord(AliyunAccessSecret);
        } catch (InvalidKeyException | NoSuchAlgorithmException ignored) {
        }
        return null;
    }

}
