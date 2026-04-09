package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@RequiredArgsConstructor
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    private final StringRedisTemplate stringRedisTemplate;
    private final IUserService userService;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follow:" + userId;
        if (isFollow) {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                stringRedisTemplate.opsForSet().add(key, id.toString());
            }
        } else {
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", id));
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key, id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isfollow(Long followUserId) {
        Integer count = query().eq("follow_user_id", followUserId).eq("user_id", UserHolder.getUser().getId()).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {

            Long userId = UserHolder.getUser().getId();
            String key1 = "follow:" + userId;
            String key2 = "follow:" + id;
            // 求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if (intersect.isEmpty()||intersect==null) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(ids);
        UserDTO userDTO = BeanUtil.copyProperties(users, UserDTO.class);
        return Result.ok(userDTO);
    }
}
