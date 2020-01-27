package com.alibaba.otter.canal.connector.core.spi;

import java.lang.annotation.*;

/**
 * SPI装载器注解
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface SPI {

    // Default SPI name
    String value() default "";
}
