package com.neoniou.tool.dag.exception;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
public class DagJobException extends RuntimeException {

    public DagJobException(String message) {
        super(message);
    }

    public DagJobException(Exception e) {
        super(e);
    }
}
