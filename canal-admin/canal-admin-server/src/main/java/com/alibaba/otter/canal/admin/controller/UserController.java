package com.alibaba.otter.canal.admin.controller;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.User;

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

    @PostMapping(value = "/login")
    public BaseModel<Map<String, String>> login(@RequestBody User user, @PathVariable String env) {
        if ("admin".equals(user.getUsername()) && "121212".equals(user.getPassword())) {
            Map<String, String> tokenResp = new HashMap<>();
            String token = UUID.randomUUID().toString();
            loginUsers.put(token, user);
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

    @PostMapping(value = "/logout")
    public BaseModel<String> logout(@PathVariable String env) {
        return BaseModel.getInstance("success");
    }
}
