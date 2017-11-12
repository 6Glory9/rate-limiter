/**
 * Caijiajia confidential
 * <p/>
 * Copyright (C) 2015 Shanghai Shuhe Co., Ltd. All rights reserved.
 * <p/>
 * No parts of this file may be reproduced or transmitted in any form or by any means,
 * electronic, mechanical, photocopying, recording, or otherwise, without prior written
 * permission of Shanghai Shuhe Co., Ltd.
 */
package cn.wukq.ratelimiter.server.form;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;

/**
 * @author wukaiqiang
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RateLimiterForm {
    @NotBlank
    private String name;
    @NotBlank
    private String context;
    @NotNull
    private Integer maxPermits;
    @NotNull
    private Integer rate;
}
