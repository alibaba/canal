package com.alibaba.otter.canal.admin.controller;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

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
import com.alibaba.otter.canal.admin.service.CanalConfigService;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.google.common.base.Joiner;

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

    @Autowired
    CanalInstanceService        canalInstanceConfigService;

    @Autowired
    CanalConfigService          canalConfigService;

    @Value(value = "${canal.adminUser}")
    String                      user;

    @Value(value = "${canal.adminPasswd}")
    String                      passwd;

    /**
     * 获取server全局配置
     */
    @GetMapping(value = "/server_polling")
    public BaseModel<CanalConfig> canalConfigPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                  @PathVariable String env, @RequestParam String md5) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }

        CanalConfig config = canalConfigService.getCanalConfig();
        if (StringUtils.isEmpty(md5)) {
            return BaseModel.getInstance(config);
        } else {

            try {
                String newMd5 = SecurityUtil.md5String(config.getContent());
                if (StringUtils.equals(md5, newMd5)) {
                    config.setContent(null);
                }
            } catch (NoSuchAlgorithmException e) {
            }

            return BaseModel.getInstance(config);
        }
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

        CanalInstanceConfig config = canalInstanceConfigService.findOne(destination);
        if (StringUtils.isEmpty(md5)) {
            return BaseModel.getInstance(config);
        } else {
            try {
                String newMd5 = SecurityUtil.md5String(config.getContent());
                if (StringUtils.equals(md5, newMd5)) {
                    config.setContent(null);
                }
            } catch (NoSuchAlgorithmException e) {
            }

            return BaseModel.getInstance(config);
        }
    }

    /**
     * 获取对应server(ip+port)所需要运行的instance列表
     */
    @GetMapping(value = "/instances_polling")
    public BaseModel<CanalInstanceConfig> instancesPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                        @PathVariable String env, @RequestParam String ip,
                                                        @RequestParam String port, @RequestParam String md5) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }

        CanalInstanceConfig canalInstanceConfig = new CanalInstanceConfig();
        List<CanalInstanceConfig> configs = canalInstanceConfigService.findList(canalInstanceConfig);
        List<String> instances = configs.stream().map(CanalInstanceConfig::getName).collect(Collectors.toList());
        String data = Joiner.on(',').join(instances);
        canalInstanceConfig.setContent(data);
        if (StringUtils.isEmpty(md5)) {
            return BaseModel.getInstance(canalInstanceConfig);
        } else {
            try {
                String newMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
                if (StringUtils.equals(md5, newMd5)) {
                    canalInstanceConfig.setContent(null);
                }
            } catch (NoSuchAlgorithmException e) {
            }

            return BaseModel.getInstance(canalInstanceConfig);
        }
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
