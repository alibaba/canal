package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.User;

public interface UserService {

    User find4Login(String username, String password);

    void update(User user);
}
