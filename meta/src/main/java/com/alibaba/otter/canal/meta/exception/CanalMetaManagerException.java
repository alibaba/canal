/**
 * Project: otter.canal.server-4.1.0
 * 
 * File Created at 2012-6-21
 * $Id$
 * 
 * Copyright 1999-2100 Alibaba.com Corporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Alibaba Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Alibaba.com.
 */
package com.alibaba.otter.canal.meta.exception;

import com.alibaba.otter.canal.common.CanalException;

/**
 * @author zebin.xuzb @ 2012-6-21
 * @version 4.1.0
 */
public class CanalMetaManagerException extends CanalException {

    private static final long serialVersionUID = -654893533794556357L;

    public CanalMetaManagerException(String errorCode){
        super(errorCode);
    }

    public CanalMetaManagerException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalMetaManagerException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalMetaManagerException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalMetaManagerException(Throwable cause){
        super(cause);
    }

}
