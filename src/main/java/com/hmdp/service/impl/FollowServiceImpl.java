package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_USER_KEY;


@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    // 关注和取关功能
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOW_USER_KEY + userId;  // userId是当前用户
        // 1、判断是关注还是取关
        if (isFollow) {
            // 2、关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);  // 关注的用户
            boolean success = save(follow);
            if (success) {
                // 把关注用户的id，放入redis的set集合
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // 3、取关，删除delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean success = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId)
            );
            if (success) {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    // 查询是否关注
    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        // 查询是否关注 select * from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        // 判断
        return Result.ok(count > 0);
    }

    // 查询共同关注
    @Override
    public Result followCommons(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key1 = FOLLOW_USER_KEY + userId;
        String key2 = FOLLOW_USER_KEY + id;
        // 求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userDTOS = userService.listByIds(ids).stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
