package com.alibaba.otter.canal.store.file.model;

public enum AppendMessageStatus {

    // 成功追加消息
    PUT_OK,
    // 走到文件末尾
    END_OF_FILE,
    // 消息大小超限
    MESSAGE_SIZE_EXCEEDED,
    // 未知错误
    UNKNOWN_ERROR,
}
