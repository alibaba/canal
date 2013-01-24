package com.alibaba.otter.canal.parse.index;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;

/**
 * 基于meta信息的实现
 * 
 * @author jianghang 2012-7-10 下午05:02:33
 * @version 1.0.0
 */
public class MetaLogPositionManager extends AbstractCanalLifeCycle implements CanalLogPositionManager {

    private static final Logger logger = LoggerFactory.getLogger(MetaLogPositionManager.class);
    private CanalMetaManager    metaManager;

    public void start() {
        super.start();
        Assert.notNull(metaManager);
        if (!metaManager.isStart()) {
            metaManager.start();
        }
    }

    public void stop() {
        super.stop();
        if (metaManager.isStart()) {
            metaManager.stop();
        }
    }

    public void persistLogPosition(String destination, LogPosition logPosition) {
        // do nothing
        logger.info("persist LogPosition:{}", destination, logPosition);
    }

    public LogPosition getLatestIndexBy(String destination) {
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentitys)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentitys) {
                LogPosition position = (LogPosition) metaManager.getCursor(clientIdentity);
                if (position == null) {
                    continue;
                }

                if (result == null) {
                    result = position;
                } else {
                    result = CanalEventUtils.min(result, position);
                }
            }
        }

        return result;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

}
