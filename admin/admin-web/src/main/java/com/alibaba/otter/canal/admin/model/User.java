package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;
import io.ebean.annotation.WhenCreated;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

/**
 * 用户信息实体类
 */
@Entity
@Table(name = "canal_user")
public class User extends Model {

    public static final UserFinder find = new UserFinder();

    public static class UserFinder extends Finder<Long, User> {

        /**
         * Construct using the default EbeanServer.
         */
        public UserFinder(){
            super(User.class);
        }

    }

    @Id
    private Long   id;
    private String username;
    private String password;
    private String roles;
    private String introduction;
    private String avatar;
    private String name;
    @WhenCreated
    private Date   creationDate;

    @Transient
    private String oldPassword;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRoles() {
        return roles;
    }

    public void setRoles(String roles) {
        this.roles = roles;
    }

    public String getIntroduction() {
        return introduction;
    }

    public void setIntroduction(String introduction) {
        this.introduction = introduction;
    }

    public String getAvatar() {
        return avatar;
    }

    public void setAvatar(String avatar) {
        this.avatar = avatar;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getOldPassword() {
        return oldPassword;
    }

    public void setOldPassword(String oldPassword) {
        this.oldPassword = oldPassword;
    }
}
