package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.model.User;
import com.alibaba.otter.canal.admin.service.UserService;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

/**
 * 用户信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Service
public class UserServiceImpl implements UserService {

    public User find4Login(String username, String password) {
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
            return null;
        }
        User user = User.find.query().where().eq("username", username).eq("password", password).findOne();
        if (user != null) {
            user.setPassword("");
        }
        return user;
    }

    public void update(User user) {
        User userTmp = User.find.byId(1L);
        if (userTmp == null) {
            throw new ServiceException();
        }
        if (!userTmp.getPassword().equals(user.getOldPassword())) {
            throw new ServiceException("错误的旧密码");
        }
        user.setId(1L);
        user.update("username", "nn:password");
    }
}
