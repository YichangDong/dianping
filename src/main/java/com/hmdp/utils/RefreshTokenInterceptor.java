package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import com.hmdp.dto.UserDTO;

import cn.hutool.core.bean.BeanUtil;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
@NoArgsConstructor
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String token = request.getHeader("authorization");
        if (token == null || token.isEmpty()) {
            return true; // 没有 token，直接放行（不阻止注册/登录等公开接口）
        }
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);
        if (userMap == null || userMap.isEmpty()) {
            return true; // token 无效，放行，让 LoginInterceptor 在受保护接口上拦截
        }
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), true);
        UserHolder.saveUser(userDTO);
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);
        return true;
    }
}