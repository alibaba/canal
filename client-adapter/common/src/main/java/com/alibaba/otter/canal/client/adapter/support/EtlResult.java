package com.alibaba.otter.canal.client.adapter.support;

import java.io.Serializable;

public class EtlResult implements Serializable {
    private static final long serialVersionUID = 4250522736289866505L;

    private boolean succeeded = false;

    private String resultMessage;

    private String errorMessage;

    public boolean getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public void setResultMessage(String resultMessage) {
        this.resultMessage = resultMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}