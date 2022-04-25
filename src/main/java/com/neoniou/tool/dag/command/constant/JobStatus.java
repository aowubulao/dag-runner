package com.neoniou.tool.dag.command.constant;

import lombok.Getter;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@Getter
public enum JobStatus {

    /**
     *
     */
    NOT_START(0, "not_start"),

    SUCCESS(1, "success"),

    ERROR(2, "error"),

    TIMEOUT(3, "timeout"),

    ABORT(4, "discard"),

    RUNNING(5, "running")

    ;

    private final Integer statusCode;
    private final String statusDesc;

    JobStatus(Integer statusCode, String statusDesc) {
        this.statusCode = statusCode;
        this.statusDesc = statusDesc;
    }
}
