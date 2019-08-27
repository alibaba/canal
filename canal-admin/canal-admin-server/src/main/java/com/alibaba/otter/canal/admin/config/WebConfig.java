package com.alibaba.otter.canal.admin.config;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.alibaba.otter.canal.admin.controller.UserController;
import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 相关MVC拦截器配置
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor() {

            @Override
            public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
                                     Object o) throws Exception {
                httpServletResponse.setHeader("Access-Control-Allow-Origin", "*");
                httpServletResponse.setHeader("Access-Control-Allow-Methods", "*");
                httpServletResponse.setHeader("Access-Control-Allow-Headers",
                    "Origin, X-Requested-With, Content-Type, Accept, Authorization, X-Token");
                httpServletResponse.setHeader("Access-Control-Allow-Credentials", "true");
                httpServletResponse.setHeader("Access-Control-Max-Age", String.valueOf(3600 * 24));

                if (HttpMethod.OPTIONS.toString().equals(httpServletRequest.getMethod())) {
                    httpServletResponse.setStatus(HttpStatus.NO_CONTENT.value());
                    return false;
                }

                return true;
            }
        }).addPathPatterns("/api/**");

        registry.addInterceptor(new HandlerInterceptor() {

            @Override
            public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
                                     Object o) throws Exception {
                String token = httpServletRequest.getHeader("X-Token");
                boolean valid = false;
                if (token != null) {
                    User user = UserController.loginUsers.getIfPresent(token);
                    if (user != null) {
                        valid = true;
                        httpServletRequest.setAttribute("user", user);
                        httpServletRequest.setAttribute("token", token);
                    }
                }
                if (!valid) {
                    BaseModel baseModel = BaseModel.getInstance(null);
                    baseModel.setCode(50014);
                    baseModel.setMessage("Expired token");
                    ObjectMapper mapper = new ObjectMapper();
                    String json = mapper.writeValueAsString(baseModel);
                    try {
                        httpServletResponse.setContentType("application/json;charset=UTF-8");
                        PrintWriter out = httpServletResponse.getWriter();
                        out.print(json);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }

                return true;
            }
        })
            .addPathPatterns("/api/**")
            .excludePathPatterns("/api/**/config/**")
            .excludePathPatterns("/api/**/user/login")
            .excludePathPatterns("/api/**/user/logout")
            .excludePathPatterns("/api/**/user/info");
    }
}
