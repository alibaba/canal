package com.alibaba.otter.canal.admin.controller;

import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.service.PollingConfigService;
import com.alibaba.otter.canal.protocol.SecurityUtil;

/**
 * Canal Instance配置管理控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}/config")
public class PollingConfigController {

    private static final byte[] seeds = "canal is best!".getBytes();

    @Value(value = "${canal.adminUser}")
    String                      user;

    @Value(value = "${canal.adminPasswd}")
    String                      passwd;

    @Autowired
    PollingConfigService        pollingConfigService;

    /**
     * 获取server全局配置
     */
    @GetMapping(value = "/server_polling")
    public BaseModel<CanalConfig> canalConfigPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                  @RequestParam String ip, @RequestParam Integer port,
                                                  @RequestParam String md5, @RequestParam boolean register,
                                                  @RequestParam String cluster, @RequestParam String name,
                                                  @PathVariable String env) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }

        if (StringUtils.isEmpty(md5) && register) {
            // do something
            pollingConfigService.autoRegister(ip, port, cluster, StringUtils.trimToNull(name));
        }

        CanalConfig canalConfig = pollingConfigService.getChangedConfig(ip, port, md5);
        return BaseModel.getInstance(canalConfig);
    }

    /**
     * 获取单个instance的配置
     */
    @GetMapping(value = "/instance_polling/{destination}")
    public BaseModel<CanalInstanceConfig> instanceConfigPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                             @PathVariable String env,
                                                             @PathVariable String destination, @RequestParam String md5) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }

        CanalInstanceConfig canalInstanceConfig = pollingConfigService.getInstanceConfig(destination, md5);
        return BaseModel.getInstance(canalInstanceConfig);
    }

    /**
     * 获取对应server(ip+port)所需要运行的instance列表
     */
    @GetMapping(value = "/instances_polling")
    public BaseModel<CanalInstanceConfig> instancesPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                        @RequestParam String ip, @RequestParam Integer port,
                                                        @RequestParam String md5, @PathVariable String env) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }

        CanalInstanceConfig canalInstanceConfig = pollingConfigService.getInstancesConfig(ip, port, md5);
        return BaseModel.getInstance(canalInstanceConfig);
    }

    private boolean auth(String user, String passwd) {
        // 如果user/passwd密码为空,则任何用户账户都能登录
        if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
            if (StringUtils.isEmpty(this.passwd)) {
                return true;
            } else if (StringUtils.isEmpty(passwd)) {
                // 如果server密码有配置,客户端密码为空,则拒绝
                return false;
            }

            try {
                // manager这里保存了原始密码，反过来和canal发送过来的进行校验
                byte[] passForClient = SecurityUtil.scramble411(this.passwd.getBytes(), seeds);
                return SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(passwd), seeds);
            } catch (NoSuchAlgorithmException e) {
                return false;
            }
        }

        return false;
    }
}
