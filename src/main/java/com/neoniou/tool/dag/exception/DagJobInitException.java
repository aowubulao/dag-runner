package com.neoniou.tool.dag.exception;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
public class DagJobInitException extends RuntimeException {

    public DagJobInitException(String message) {
        super(message);
    }

    public DagJobInitException(Exception e) {
        super(e);
    }
}
