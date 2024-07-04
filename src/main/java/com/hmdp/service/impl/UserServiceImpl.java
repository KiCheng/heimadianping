package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;


@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送手机验证码
     *
     * @param phone
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1、校验手机号格式
        if (!RegexUtils.isPhoneInvalid(phone)) {
            // 2、手机号格式错误，返回错误信息
            return Result.fail("手机号格式错误!");
        }

        // 3、生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4、保存验证码到redis中
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);  // 业务前缀 + 数据有效期

        // 5、发送验证码到前端
        log.debug("发送短信验证码成功，验证码：" + code);

        return Result.ok();
    }

    /**
     * 登录、注册功能
     *
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1、校验手机号格式（每一步都要做格式校验）
        String phone = loginForm.getPhone();
        if (!RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }

        // 2、从redis获取验证码并校验验证码
        String code = loginForm.getCode();  // 表单提交的验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 验证码不一致，返回错误信息
            return Result.fail("验证码错误!");
        }

        // 3、根据手机号查询用户  select * from tb_user where phone = ?
        User user = query().eq("phone", phone).one();
        if (user == null) {
            // 用户不存在，创建新的用户并保存到数据库
            user = createUserWithPhone(phone);
        }

        // 4、保存用户数据到redis中
        // 4.1、随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString();
        // 4.2、将User对象转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        // 4.3、存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        // 4.4、保存token在redis中的有效期
        stringRedisTemplate.expire(tokenKey, 30L, TimeUnit.MINUTES);

        // 5、返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2. 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = USER_SIGN_KEY + userId + ":" + keySuffix;
        // 4. 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();  // 1~31
        // 5. 写入Redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2. 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = USER_SIGN_KEY + userId + ":" + keySuffix;
        // 4. 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();  // 1~31

        // 5.获取本月截止今天为止所有的签到记录，返回的是一个10进制数
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create()
                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        // 取当天的签到结果
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        // 6.循环遍历
        int count = 0;
        while (true) {
            if ((num & 1) == 0) {
                break;
            } else {
                count++;
            }
            num = num >> 1;
        }
        return Result.ok(count);
    }

    /**
     * 根据手机号创建新用户
     *
     * @param phone
     * @return
     */
    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_" + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
