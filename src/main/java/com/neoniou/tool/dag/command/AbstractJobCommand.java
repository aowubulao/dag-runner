package com.neoniou.tool.dag.command;

import com.neoniou.tool.dag.annotation.DagJob;
import com.neoniou.tool.dag.command.constant.JobStatus;
import com.neoniou.tool.dag.exception.DagJobInitException;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
public abstract class AbstractJobCommand implements JobInterface {

    protected final List<String> inputKeys;
    protected final List<String> outputKeys;
    protected final long timeout;

    @Getter
    @Setter
    protected JobStatus jobStatus;

    protected AbstractJobCommand() {
        DagJob annotation = this.getClass().getAnnotation(DagJob.class);
        if (annotation == null) {
            throw new DagJobInitException("No args construct need DagJob annotation to init");
        }
        this.inputKeys = Arrays.asList(annotation.inputKeys());
        this.outputKeys = Arrays.asList(annotation.outputKeys());
        this.timeout = annotation.timeout();
        jobStatus = JobStatus.NOT_START;
    }

    protected AbstractJobCommand(String[] inputKeys, String[] outputKeys) {
        this(inputKeys, outputKeys, DEFAULT_NODE_TIMEOUT);
    }

    protected AbstractJobCommand(String[] inputKeys, String[] outputKeys, long timeout) {
        this.inputKeys = Arrays.asList(inputKeys);
        this.outputKeys = Arrays.asList(outputKeys);
        this.timeout = timeout;
    }

    @Override
    public long getJobTimeout() {
        return timeout;
    }

    @Override
    public List<String> getJobInputKeys() {
        return this.inputKeys;
    }

    @Override
    public List<String> getJobOutputKeys() {
        return this.outputKeys;
    }
}
