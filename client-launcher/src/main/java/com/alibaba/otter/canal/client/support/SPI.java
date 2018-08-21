package com.alibaba.otter.canal.client.support;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface SPI {
    //Default SPI name
    String value() default "";
}
