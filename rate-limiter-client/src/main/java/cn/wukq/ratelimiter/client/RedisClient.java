/**
 * Caijiajia confidential
 * <p>
 * Copyright (C) 2016 Shanghai Shuhe Co., Ltd. All rights reserved.
 * <p>
 * No parts of this file may be reproduced or transmitted in any form or by any means,
 * electronic, mechanical, photocopying, recording, or otherwise, without prior written
 * permission of Shanghai Shuhe Co., Ltd.
 */
package cn.wukq.ratelimiter.client;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * A wrapper of the Spring redis client.
 *
 * @author wukaiqiang
 */
public class RedisClient implements InitializingBean {

    private String hostName;
    private int port;
    private String password;
    private int database = 0;
    private boolean usePool = true;
    private int maxConnTotal = 100;
    private int maxConnIdle = 10;

    private static Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedisScript<List<Object>> redisScript;

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        JedisConnectionFactory connectionFactory = null;
        if (usePool) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(maxConnTotal);
            poolConfig.setMaxIdle(maxConnIdle);
            connectionFactory = new JedisConnectionFactory(poolConfig);
        } else {
            connectionFactory = new JedisConnectionFactory();
        }
        connectionFactory.setHostName(hostName);
        connectionFactory.setPort(port);
        if (StringUtils.isNotBlank(password)) {
            connectionFactory.setPassword(password);
        }
        connectionFactory.setDatabase(database);
        connectionFactory.afterPropertiesSet();

        redisTemplate = new StringRedisTemplate();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
    }
    
    public StringRedisTemplate getRedisTemplate() {
    	return redisTemplate;
    }
    
    /* 原子计数器 */
    public Long increment(String key, long i) {
        return redisTemplate.opsForValue().increment(key, i);
    }

    /* Key commands */
    public void delete(String key) {
        redisTemplate.delete(key);
    }

    public void delete(String... keys) {
        redisTemplate.delete(Arrays.asList(keys));
    }

    public Set<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }   
    /* End of Key commands */

    /* Value commands */
    public void set(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    public String get(String key) {
        return redisTemplate.opsForValue().get(key);
    }
    /* End of Value commands */

    /* Set commands */
    public long opsForSetAdd(String key, String... members) {
        return redisTemplate.opsForSet().add(key, members);
    }

    public Set<String> opsForSetIntersect(String key1, String key2) {
        return redisTemplate.opsForSet().intersect(key1, key2);
    }

    public Long opsForSetIntersectAndStore(String destKey, String key1, String key2) {
        return redisTemplate.opsForSet().intersectAndStore(key1, key2, destKey);
    }

    public Set<String> opsForSetUnion(String key1, String key2) {
        return redisTemplate.opsForSet().union(key1, key2);
    }

    public Long opsForSetUnionAndStore(String destKey, String key1, String key2) {
        return redisTemplate.opsForSet().unionAndStore(key1, key2, destKey);
    }

    public Set<String> opsForSetDifference(String key1, String key2) {
        return redisTemplate.opsForSet().difference(key1, key2);
    }

    public Long opsForSetDifferenceAndStore(String destKey, String key1, String key2) {
        return redisTemplate.opsForSet().differenceAndStore(key1, key2, destKey);
    }

    public Long opsForSetSize(String key) {
        return redisTemplate.opsForSet().size(key);
    }

    public Set<String> opsForSetMembers(String key) {
        return redisTemplate.opsForSet().members(key);
    }
    /* Ends of Set commands */

    /* SortedSet commands */
    public Set<String> opsForSortedSetMembers(String key) {
        long setLength = redisTemplate.opsForZSet().size(key);
        return redisTemplate.opsForZSet().range(key, 0, setLength);
    }
    /* Ends of SortedSet commands */

    /* Hash commands */
    public Map<String, Object> opsForHashEntries(String key) {
        Map<String, Object> entries = new HashMap<>();
        Map<Object, Object> entryMap = redisTemplate.opsForHash().entries(key);
        if(entryMap != null){
            for(Map.Entry<Object, Object> entry : entryMap.entrySet()){
                String entryKey = entry.getKey().toString();
                Object entryValue = entry.getValue();
                entries.put(entryKey, entryValue);
            }
        }
        return entries;
    }

    public Set<Object> opsForHashKeys(String key) {
        return redisTemplate.opsForHash().keys(key);
    }

    public List<Object> opsForHashValues(String key) {
        return redisTemplate.opsForHash().values(key);
    }

    public Object opsForHashGet(String key, Object property) {
        return redisTemplate.opsForHash().get(key, property);
    }

    public boolean opsForHashHasKey(String key, Object property) {
        return redisTemplate.opsForHash().hasKey(key, property);
    }

    public long opsForHashSize(String key) {
        return redisTemplate.opsForHash().size(key);
    }

    public List<Object> opsForHashMultiGet(String key, Collection<Object> properties) {
        return redisTemplate.opsForHash().multiGet(key, properties);
    }
    /* Ends of Hash commands 	*/

    /* Custom commands */
    public long calcLogicExpr(String resultKey, String logicExpr, boolean isSortedSetCalc) {

        List<Object> results = redisTemplate.execute(redisScript, Collections.singletonList(resultKey), logicExpr, isSortedSetCalc ? "true" : "false");
        long count = (Long) results.get(0);
        String respMsg = (String) results.get(1);
        if (count == -1) {
            logger.error("redis script about calculating logic expr failed: " + respMsg);
        }
        return count;
    }
    /* Ends of Custom commands */

    /**
     * @return the hostName
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * @param hostName the hostName to set
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the database
     */
    public int getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public void setDatabase(int database) {
        this.database = database;
    }

    /**
     * @return the usePool
     */
    public boolean isUsePool() {
        return usePool;
    }

    /**
     * @param usePool the usePool to set
     */
    public void setUsePool(boolean usePool) {
        this.usePool = usePool;
    }

    /**
     * @return the maxConnTotal
     */
    public int getMaxConnTotal() {
        return maxConnTotal;
    }

    /**
     * @param maxConnTotal the maxConnTotal to set
     */
    public void setMaxConnTotal(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    /**
     * @return the maxConnIdle
     */
    public int getMaxConnIdle() {
        return maxConnIdle;
    }

    /**
     * @param maxConnIdle the maxConnIdle to set
     */
    public void setMaxConnIdle(int maxConnIdle) {
        this.maxConnIdle = maxConnIdle;
    }

}


