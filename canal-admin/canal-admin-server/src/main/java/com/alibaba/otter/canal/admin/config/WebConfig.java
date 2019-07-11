package com.alibaba.otter.canal.admin.config;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.alibaba.otter.canal.admin.controller.UserController;
import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.PrintWriter;

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
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return false;
                }

                return true;
            }
        }).addPathPatterns("/api/**").excludePathPatterns("/api/**/user/**");
    }

    /**
     * 跨域支持
     *
     * @param registry
     */
    // @Override
    // public void addCorsMappings(CorsRegistry registry) {
    // registry.addMapping("/api")
    // .allowedOrigins("http://127.0.0.1")
    // .allowCredentials(true)
    // .allowedMethods("*")
    // .allowedHeaders("Origin, X-Requested-With, Content-Type, Accept,
    // Authorization")
    // .maxAge(3600 * 24);
    // }

    /**
     * 添加静态资源--过滤swagger-api (开源的在线API文档)
     *
     * @param registry
     */
    // @Override
    // public void addResourceHandlers(ResourceHandlerRegistry registry) {
    // // 过滤swagger
    // registry.addResourceHandler("swagger-ui.html").addResourceLocations("classpath:/META-INF/resources/");
    //
    // registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
    //
    // registry.addResourceHandler("/swagger-resources/**")
    // .addResourceLocations("classpath:/META-INF/resources/swagger-resources/");
    //
    // registry.addResourceHandler("/swagger/**").addResourceLocations("classpath:/META-INF/resources/swagger*");
    //
    // registry.addResourceHandler("/v2/api-docs/**")
    // .addResourceLocations("classpath:/META-INF/resources/v2/api-docs/");
    //
    // }
}
