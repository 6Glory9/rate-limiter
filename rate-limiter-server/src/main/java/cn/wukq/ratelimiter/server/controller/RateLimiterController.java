/**
 * Caijiajia confidential
 * <p/>
 * Copyright (C) 2015 Shanghai Shuhe Co., Ltd. All rights reserved.
 * <p/>
 * No parts of this file may be reproduced or transmitted in any form or by any means,
 * electronic, mechanical, photocopying, recording, or otherwise, without prior written
 * permission of Shanghai Shuhe Co., Ltd.
 */
package cn.wukq.ratelimiter.server.controller;

import cn.wukq.ratelimiter.server.form.RateLimiterForm;
import cn.wukq.ratelimiter.server.service.RateLimiterService;
import cn.wukq.ratelimiter.server.vo.RateLimiterVo;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


/**
 * @author wukaiqiang
 */
@RestController
public class RateLimiterController {
    @Autowired
    private RateLimiterService rateLimiterService;

    @RequestMapping(value = "/rate-limiters")
    public List<RateLimiterVo> getRateLimiters(@RequestParam String context) {
        return rateLimiterService.getRateLimiters(context);
    }

    @RequestMapping(value = "/rate-limiters", method = RequestMethod.POST)
    public void saveOrUpdateRateLimiter(@RequestBody RateLimiterForm form) {
        rateLimiterService.saveOrUpdateRateLimiter(form);
    }


    @RequestMapping(value = "/rate-limiters/{context}/{name}", method = RequestMethod.DELETE)
    public void deleteRateLimiter(@PathVariable String context, @PathVariable String name) {
        rateLimiterService.deleteRateLimiter(context, name);
    }


    @RequestMapping(value = "/rate-limiters/test", method = RequestMethod.POST)
    public Map<String, Object> testRateLimiter(@RequestBody JSONObject jsonObject) {
        return rateLimiterService.testRateLimiter(jsonObject);
    }


}