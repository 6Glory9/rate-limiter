package cn.wukq.ratelimiter.client;


import cn.wukq.ratelimiter.client.RateLimiterClient;
import cn.wukq.ratelimiter.client.RateLimiterConstants;
import cn.wukq.ratelimiter.client.RedisClient;
import cn.wukq.ratelimiter.client.Token;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-ratelimiter-test.xml"})
public class RateLimiterClientTest {
    @Autowired
    private RateLimiterClient rateLimiterClient;

    @Resource(name = "rateLimiterRedisClient")
    private RedisClient redisClient;

    @Resource(name = "rateLimiterLua")
    private RedisScript<Long> rateLimiterLua;


    @Test
    public void testAcquirePass() {
        // 数据准备
        redisClient.getRedisTemplate()
                .execute(rateLimiterLua,
                        ImmutableList.of(RateLimiterConstants.RATE_LIMITER_KEY_PREFIX + "wukq:test", RateLimiterConstants.RATE_LIMITER_INIT_METHOD),
                        "1", "1", "test");

        // 调用方法，并且断言
        Assert.assertEquals(rateLimiterClient.acquire("test", "wukaiqiang:test", 1), Token.PASS);


        // 现场恢复
        redisClient.getRedisTemplate()
                .execute(rateLimiterLua,
                        ImmutableList.of(RateLimiterConstants.RATE_LIMITER_KEY_PREFIX + "wukaiqiang:test", RateLimiterConstants.RATE_LIMITER_DELETE_METHOD));
    }
}
