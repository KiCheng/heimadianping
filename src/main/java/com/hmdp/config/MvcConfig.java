package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 * MVC配置类
 * @author KiCheng
 * @date 2024/6/4
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 目的是使拦截器生效
     * @param registry
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 登录token校验拦截器
        registry.addInterceptor(new LoginInterceptor(stringRedisTemplate))
                .excludePathPatterns(
                        "/user/code",
                        "/user/login",
                        "/shop/**",
                        "/voucher/**",
                        "shop-type/**",
                        "/upload/**",
                        "/blog/hot"
                ).order(1);

        // 刷新token缓存有效期拦截器
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}
