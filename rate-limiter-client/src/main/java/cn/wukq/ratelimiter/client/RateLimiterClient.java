package cn.wukq.ratelimiter.client;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
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

    @Resource(name = "rateLimiterRedisClient")
    private RedisClient redisClient;

    @Resource(name = "rateLimiterLua")
    private RedisScript<Long> rateLimiterClientLua;


    /**
     * 获取令牌，访问redis异常算做成功
     * 默认的permits为1
     *
     * @param app
     * @param key
     * @return
     */
    public boolean acquire(String app, String key) {
        Assert.notNull(key);
        Token token = acquireToken(app, key);
        return token.isPass() || token.isAccessRedisFail();
    }


    /**
     * 获取{@link Token}
     * 默认的permits为1
     *
     * @param app
     * @param key
     * @return
     */
    public Token acquireToken(String app, String key) {
        Assert.notNull(app);
        Assert.notNull(key);
        return acquireToken(app, key, 1);
    }

    /**
     * 获取{@link Token}
     *
     * @param app
     * @param key
     * @param permits
     * @return
     */
    public Token acquireToken(String app, String key, Integer permits) {
        Assert.notNull(app);
        Assert.notNull(key);
        Assert.notNull(permits);
        Token token;
        try {
            Long currMillSecond = redisClient.getRedisTemplate().execute(new RedisCallback<Long>() {
                @Override
                public Long doInRedis(RedisConnection connection) throws DataAccessException {
                    return connection.time();
                }
            });
            Long acquire = redisClient.getRedisTemplate().execute(rateLimiterClientLua, ImmutableList.of(getKey(key)), RateLimiterConstants.RATE_LIMITER_ACQUIRE_METHOD, permits.toString(), currMillSecond.toString(), app);

            if (acquire == 1) {
                token = Token.PASS;
            } else if (acquire == -1) {
                token = Token.FUSING;
            } else {
                logger.error("no rate limit config for app={}", app);
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
