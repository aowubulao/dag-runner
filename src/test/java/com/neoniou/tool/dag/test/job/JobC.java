package com.neoniou.tool.dag.test.job;

import com.neoniou.tool.dag.annotation.DagJob;
import com.neoniou.tool.dag.command.job.SimpleJobCommand;

import java.util.Map;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@DagJob(inputKeys = {"b", "z"}, outputKeys = {"u", "v"}, timeout = 100)
public class JobC extends SimpleJobCommand {

    @Override
    public void runJob(Map<String, Object> inputMap, Map<String, Object> outputMap) {
        System.out.printf("%s execute, inputMap: %s%n", this.getClass().getSimpleName(), inputMap.toString());
        outputMap.put("u", "jobC-u");
        outputMap.put("v", "jobC-v");
    }
}
