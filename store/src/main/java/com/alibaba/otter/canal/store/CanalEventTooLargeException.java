package com.alibaba.otter.canal.store;


import com.alibaba.otter.canal.common.CanalException;

public class CanalEventTooLargeException extends CanalException {

    public CanalEventTooLargeException(Throwable cause) {
        super(cause);
    }
	private static final long serialVersionUID = 8364396631052101010L;

}
