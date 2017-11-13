# rate-limiter
> 基于redis限流系统
## 使用

#### 1、引入依赖

```xml
<dependency>
    <groupId>cn.wukq</groupId>
    <artifactId>rate-limiter-client</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>

```
#### 2、引入applicationContext

```xml
<import resource="classpath:applicationContext-rateLimiter.xml"/>
```
```
## spring-config.properties 中加入
redis.host.user.ratelimiter=127.0.0.1
redis.port.user.ratelimiter=6379
redis.database.user.ratelimiter=4
```

#### 3、代码中

```java
@Resource(name = "rateLimiterRedisClient")
private RedisClient redisClient;


/**
* 这个方法在取令牌过程中，如果redis挂了也算成功
* 
* 取令牌的数量为默认值：1
*/
boolean acquire = redisClient.acquire(key);



/**
* 
* 
* 这个方法会返回一个Token 对象，
* Token对象有详细的描述告知取令牌的状态
* 
*  取令牌的数量为：tokenCount
* 
*/
Token token = redisClient.acquireToken(key,tokenCount);

```


