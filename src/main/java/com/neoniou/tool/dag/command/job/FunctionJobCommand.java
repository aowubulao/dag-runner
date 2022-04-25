package com.neoniou.tool.dag.command.job;

import java.util.Map;

/**
 * @author Neo.Zzj
 * @date 2022/1/21
 */
public class FunctionJobCommand extends SimpleJobCommand {

    private final RunJobInterface runJobInterface;

    public FunctionJobCommand(RunJobInterface runJobInterface, String[] inputKeys, String[] outputKeys) {
        super(inputKeys, outputKeys);
        this.runJobInterface = runJobInterface;
    }

    public FunctionJobCommand(RunJobInterface runJobInterface, String[] inputKeys, String[] outputKeys, long timeout) {
        super(inputKeys, outputKeys, timeout);
        this.runJobInterface = runJobInterface;
    }

    @Override
    public void runJob(Map<String, Object> inputMap, Map<String, Object> outputMap) throws Exception {
        runJobInterface.run(inputMap, outputMap);
    }

    @FunctionalInterface
    public interface RunJobInterface {
        /**
         * Task
         *
         * @throws Exception e
         */
        @SuppressWarnings("all")
        void run(Map<String, Object> inputMap, Map<String, Object> outputMap) throws Exception;
    }
}
