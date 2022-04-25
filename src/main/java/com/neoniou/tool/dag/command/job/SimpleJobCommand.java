package com.neoniou.tool.dag.command.job;

import com.neoniou.tool.dag.command.AbstractJobCommand;
import com.neoniou.tool.dag.command.constant.JobStatus;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@Slf4j
@NoArgsConstructor
public abstract class SimpleJobCommand extends AbstractJobCommand {

    private final AtomicBoolean isStart = new AtomicBoolean(false);

    protected SimpleJobCommand(String[] inputKeys, String[] outputKeys) {
        super(inputKeys, outputKeys);
    }

    protected SimpleJobCommand(String[] inputKeys, String[] outputKeys, long timeout) {
        super(inputKeys, outputKeys, timeout);
    }

    @Override
    public boolean notifyJob(Map<String, Object> inputMap, Map<String, Object> outputMap) {
        if (!isStart.compareAndSet(false, true)) {
            return false;
        }
        jobStatus = JobStatus.RUNNING;
        try {
            runJob(inputMap, outputMap);
            jobStatus = JobStatus.ERROR;
        } catch (Exception e) {
            log.error("Job [{}] execute error: ", this.getClass().getName(), e);
            return false;
        }
        return true;
    }
}
