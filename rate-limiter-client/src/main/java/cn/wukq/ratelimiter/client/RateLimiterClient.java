package cn.wukq.ratelimiter.client;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

/**
 * @author wukaiqiang
 */
@Component
public class RateLimiterClient {

    private Logger logger = LoggerFactory.getLogger(RateLimiterClient.class);

    @Resource(name = "jasmineRedisClient")
    private RedisClient redisClient;

    @Resource(name = "rateLimiterLua")
    private RedisScript<Long> rateLimiterLua;


    /**
     * 如果是取token时访问redis异常也算成功
     * 默认的permits为1
     *
     * @param context
     * @param key
     * @return
     */
    public boolean acquire(String context, String key) {
        Assert.notNull(key);
        Token token = acquire(context, key, 1);
        return token.isPass() || token.isAccessRedisFail();
    }

    /**
     * 获取{@link Token}
     *
     * @param context
     * @param key
     * @param permits
     * @return
     */
    public Token acquire(String context, String key, Integer permits) {
        Assert.notNull(context);
        Assert.notNull(key);
        Assert.notNull(permits);
        Token token;
        try {
            Long acquire = redisClient.getRedisTemplate().execute(rateLimiterLua,
                    ImmutableList.of(getKey(key), RateLimiterConstants.RATE_LIMITER_ACQUIRE_METHOD),
                    permits.toString(), System.currentTimeMillis() + "", context);

            if (acquire == 1) {
                token = Token.PASS;
            } else if (acquire == -1) {
                token = Token.FUSING;
            } else {
                logger.error("no rate limit config for context:{}", context);
                token = Token.NO_CONFIG;
            }
        } catch (Throwable e) {
            logger.error("get rate limit token from redis error,key=" + key, e);
            token = Token.ACCESS_REDIS_FAIL;
        }
        return token;
    }

    private String getKey(String key) {
        return RateLimiterConstants.RATE_LIMITER_KEY_PREFIX + key;
    }


}
