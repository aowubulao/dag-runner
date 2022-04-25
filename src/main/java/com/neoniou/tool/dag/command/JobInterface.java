package com.neoniou.tool.dag.command;

import java.util.List;
import java.util.Map;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
public interface JobInterface {

    /**
     * Default timeout: 10s
     */
    long DEFAULT_NODE_TIMEOUT = 10000;

    /**
     * Notify job to run
     *
     * @param inputMap  Job inputMap
     * @param outputMap job outputMap
     * @return is job start success
     */
    boolean notifyJob(Map<String, Object> inputMap, Map<String, Object> outputMap);

    /**
     * Get job timeout
     *
     * @return timeout mills
     */
    long getJobTimeout();

    /**
     * Execute method
     *
     * @param inputMap  input
     * @param outputMap output
     * @throws Exception e
     */
    @SuppressWarnings("all")
    void runJob(Map<String, Object> inputMap, Map<String, Object> outputMap) throws Exception;

    /**
     * Get method
     *
     * @return job input keys
     */
    List<String> getJobInputKeys();

    /**
     * Get method
     *
     * @return job output keys
     */
    List<String> getJobOutputKeys();
}
