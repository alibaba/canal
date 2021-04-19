package com.alibaba.otter.canal.deployer.admin;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.CanalAdmin;
import com.alibaba.otter.canal.common.utils.FileUtils;
import com.alibaba.otter.canal.deployer.CanalStarter;
import com.alibaba.otter.canal.deployer.InstanceConfig;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.ManagerInstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.google.common.base.Joiner;

/**
 * 提供canal admin的管理操作
 * 
 * @author agapple 2019年8月24日 下午11:39:01
 * @since 1.1.4
 */
public class CanalAdminController implements CanalAdmin {

    private static final Logger logger = LoggerFactory.getLogger(CanalAdminController.class);
    private String              user;
    private String              passwd;
    private CanalStarter         canalStater;

    public CanalAdminController(CanalStarter canalStater){
        this.canalStater = canalStater;
    }

    @Override
    public boolean check() {
        return canalStater.isRunning();
    }

    @Override
    public synchronized boolean start() {
        try {
            if (!canalStater.isRunning()) {
                canalStater.start();
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean stop() {
        try {
            if (canalStater.isRunning()) {
                canalStater.stop(true);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean restart() {
        stop();
        return start();
    }

    @Override
    public boolean auth(String user, String passwd, byte[] seed) {
        // 如果user/passwd密码为空,则任何用户账户都能登录
        if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
            if (StringUtils.isEmpty(this.passwd)) {
                return true;
            } else if (StringUtils.isEmpty(passwd)) {
                // 如果server密码有配置,客户端密码为空,则拒绝
                return false;
            }

            try {
                byte[] passForClient = SecurityUtil.hexStr2Bytes(passwd);
                return SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(this.passwd), seed);
            } catch (NoSuchAlgorithmException e) {
                return false;
            }
        }

        return false;
    }

    @Override
    public String getRunningInstances() {
        try {
            Map<String, CanalInstance> instances = CanalServerWithEmbedded.instance().getCanalInstances();
            List<String> runningInstances = new ArrayList<>();
            instances.forEach((destination, instance) -> {
                if (instance.isStart()) {
                    runningInstances.add(destination);
                }
            });

            return Joiner.on(",").join(runningInstances);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    @Override
    public boolean checkInstance(String destination) {
        Map<String, CanalInstance> instances = CanalServerWithEmbedded.instance().getCanalInstances();
        if (instances == null || !instances.containsKey(destination)) {
            return false;
        } else {
            CanalInstance instance = instances.get(destination);
            return instance.isStart();
        }
    }

    @Override
    public boolean startInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.start(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean stopInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.stop(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean releaseInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.release(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean restartInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.reload(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public String listCanalLog() {
        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(new File("../logs/canal/"),
            TrueFileFilter.TRUE,
            TrueFileFilter.TRUE);
        List<String> names = files.stream().map(File::getName).collect(Collectors.toList());
        return Joiner.on(",").join(names);
    }

    @Override
    public String canalLog(int lines) {
        return FileUtils.readFileFromOffset("../logs/canal/canal.log", lines, "UTF-8");
    }

    @Override
    public String listInstanceLog(String destination) {
        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(new File("../logs/" + destination + "/"),
            TrueFileFilter.TRUE,
            TrueFileFilter.TRUE);
        List<String> names = files.stream().map(File::getName).collect(Collectors.toList());
        return Joiner.on(",").join(names);
    }

    @Override
    public String instanceLog(String destination, String fileName, int lines) {
        if (StringUtils.isEmpty(fileName)) {
            fileName = destination + ".log";
        }
        return FileUtils.readFileFromOffset("../logs/" + destination + "/" + fileName, lines, "UTF-8");
    }

    private InstanceAction getInstanceAction(String destination) {
        Map<InstanceConfig.InstanceMode, InstanceConfigMonitor> monitors = canalStater.getController()
            .getInstanceConfigMonitors();

        InstanceAction instanceAction = null;
        if (monitors.containsKey(InstanceConfig.InstanceMode.SPRING)) {
            SpringInstanceConfigMonitor monitor = (SpringInstanceConfigMonitor) monitors.get(InstanceConfig.InstanceMode.SPRING);
            Map<String, InstanceAction> instanceActions = monitor.getActions();
            instanceAction = instanceActions.get(destination);
        }

        if (instanceAction != null) {
            return instanceAction;
        }

        if (monitors.containsKey(InstanceConfig.InstanceMode.MANAGER)) {
            ManagerInstanceConfigMonitor monitor = (ManagerInstanceConfigMonitor) monitors.get(InstanceConfig.InstanceMode.MANAGER);
            Map<String, InstanceAction> instanceActions = monitor.getActions();
            instanceAction = instanceActions.get(destination);
        }
        return instanceAction;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public void setCanalStater(CanalStarter canalStater) {
        this.canalStater = canalStater;
    }

}
