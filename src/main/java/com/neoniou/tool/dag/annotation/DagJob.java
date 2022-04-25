package com.neoniou.tool.dag.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark on the class of the dag task
 *
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DagJob {

    String[] inputKeys();

    String[] outputKeys();

    int timeout() default 10000;
}
