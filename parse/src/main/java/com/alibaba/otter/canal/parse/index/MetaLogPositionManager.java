package com.alibaba.otter.canal.parse.index;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;

/**
 * Created by yinxiu on 17/3/18. Email: marklin.hz@gmail.com
 */
public class MetaLogPositionManager extends AbstractLogPositionManager {

    private final static Logger    logger = LoggerFactory.getLogger(MetaLogPositionManager.class);

    private final CanalMetaManager metaManager;

    public MetaLogPositionManager(CanalMetaManager metaManager){
        if (metaManager == null) {
            throw new NullPointerException("null metaManager");
        }

        this.metaManager = metaManager;
    }

    @Override
    public void stop() {
        super.stop();

        if (metaManager.isStart()) {
            metaManager.stop();
        }
    }

    @Override
    public void start() {
        super.start();

        if (!metaManager.isStart()) {
            metaManager.start();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        List<ClientIdentity> clientIdentities = metaManager.listAllSubscribeInfo(destination);
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentities)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentities) {
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

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        // do nothing
        logger.info("destination [{}] persist LogPosition:{}", destination, logPosition);
    }
}
