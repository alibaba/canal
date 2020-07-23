package com.alibaba.otter.canal.admin.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.User;
import com.alibaba.otter.canal.admin.service.UserService;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * 用户管理控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}/user")
public class UserController {

    public static final LoadingCache<String, User> loginUsers = Caffeine.newBuilder()
                                                                  .maximumSize(10_000)
                                                                  .expireAfterAccess(30, TimeUnit.MINUTES)
                                                                  .build(key -> null); // 用户登录信息缓存

    @Autowired
    UserService                                    userService;

    /**
     * 用户登录
     *
     * @param user 账号密码
     * @param env 环境变量
     * @return token
     */
    @PostMapping(value = "/login")
    public BaseModel<Map<String, String>> login(@RequestBody User user, @PathVariable String env) {
        User loginUser = userService.find4Login(user.getUsername(), user.getPassword());
        if (loginUser != null) {
            Map<String, String> tokenResp = new HashMap<>();
            String token = UUID.randomUUID().toString();
            loginUsers.put(token, loginUser);
            tokenResp.put("token", token);
            return BaseModel.getInstance(tokenResp);
        } else {
            BaseModel<Map<String, String>> model = BaseModel.getInstance(null);
            model.setCode(40001);
            model.setMessage("Invalid username or password");
            return model;
        }
    }

    /**
     * 获取用户信息
     *
     * @param token token
     * @param env 环境变量
     * @return 用户信息
     */
    @GetMapping(value = "/info")
    public BaseModel<User> info(@RequestParam String token, @PathVariable String env) {
        User user = loginUsers.getIfPresent(token);
        if (user != null) {
            return BaseModel.getInstance(user);
        } else {
            BaseModel<User> model = BaseModel.getInstance(null);
            model.setCode(50014);
            model.setMessage("Invalid token");
            return model;
        }
    }

    /**
     * 修改用户信息
     *
     * @param user 用户信息
     * @param env 环境变量
     * @param httpServletRequest httpServletRequest
     * @return 是否成功
     */
    @PutMapping(value = "")
    public BaseModel<String> update(@RequestBody User user, @PathVariable String env,
                                    HttpServletRequest httpServletRequest) {
        userService.update(user);
        String token = (String) httpServletRequest.getAttribute("token");
        loginUsers.put(token, user);
        return BaseModel.getInstance("success");
    }

    /**
     * 用户退出
     *
     * @param env 环境变量
     * @return 是否成功
     */
    @PostMapping(value = "/logout")
    public BaseModel<String> logout(@PathVariable String env) {
        return BaseModel.getInstance("success");
    }
}
