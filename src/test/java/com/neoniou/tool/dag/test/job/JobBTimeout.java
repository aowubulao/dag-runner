package com.neoniou.tool.dag.test.job;

import com.neoniou.tool.dag.annotation.DagJob;
import com.neoniou.tool.dag.command.job.SimpleJobCommand;

import java.util.Map;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@DagJob(inputKeys = {"a", "w"}, outputKeys = {"b"}, timeout = 100)
public class JobBTimeout extends SimpleJobCommand {

    @Override
    @SuppressWarnings("all")
    public void runJob(Map<String, Object> inputMap, Map<String, Object> outputMap) throws InterruptedException {
        Thread.sleep(1000);
        System.out.printf("%s execute, inputMap: %s%n", this.getClass().getSimpleName(), inputMap.toString());
        outputMap.put("b", "jobB-b");
    }
}
