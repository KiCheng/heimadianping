package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

/**
 * 登录校验拦截器
 *
 * @author KiCheng
 * @date 2024/6/4
 */
public class LoginInterceptor implements HandlerInterceptor {
    /*
     * 因为这里的LoginInterceptor拦截器是new出来的，所以不能通过`@Autowired`或者`@Resource`自动注入，只能通过自建构造方法来做
     */
    private StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 通过判断ThreadLocal中是否有用户来确定是否需要进行拦截
        if (UserHolder.getUser() == null) {
            // 需要进行拦截
            response.setStatus(401);  // 设置401状态码
            return false;
        }
        // 有用户信息，放行
        return true;
    }

}
