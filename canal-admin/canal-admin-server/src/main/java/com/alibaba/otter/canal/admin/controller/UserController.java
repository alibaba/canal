package com.alibaba.otter.canal.admin.controller;

import com.alibaba.otter.canal.admin.service.UserService;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.User;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/{env}/user")
public class UserController {

    public static final LoadingCache<String, User> loginUsers = Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build(key -> null);

    @Autowired
    UserService                                    userService;

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

    @PutMapping(value = "")
    public BaseModel<String> update(@RequestBody User user, @PathVariable String env,
                                    HttpServletRequest httpServletRequest) {
        userService.update(user);
        String token = (String) httpServletRequest.getAttribute("token");
        loginUsers.put(token, user);
        return BaseModel.getInstance("success");
    }

    @PostMapping(value = "/logout")
    public BaseModel<String> logout(@PathVariable String env) {
        return BaseModel.getInstance("success");
    }
}
