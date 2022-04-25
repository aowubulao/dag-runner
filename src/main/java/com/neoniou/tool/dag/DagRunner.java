package com.neoniou.tool.dag;

import com.neoniou.tool.dag.command.JobInterface;
import com.neoniou.tool.dag.exception.DagJobException;
import com.neoniou.tool.dag.exception.DagJobInitException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
@Slf4j
public class DagRunner {

    /**
     * Mark the task has ended <br/>
     * The task will end under the following conditions: <br/>
     * 1. All jobs are ended <br/>
     * 2. Some jobs timed out
     */
    private final AtomicBoolean isEnd = new AtomicBoolean(false);

    private final AtomicInteger endJobCount = new AtomicInteger(0);

    private final int jobCount;

    /**
     * Input keys, used to check start
     */
    private final String[] inputKeys;
    /**
     * Output keys, used to check end
     */
    private final List<String> outputKeys;

    private final Executor executors;

    private long totalTimeout;

    /**
     * Store job output
     */
    private final Map<String, Object> outputStorageMap = new ConcurrentHashMap<>(32);

    /**
     * Index
     */
    private final Map<String, JobInterface> inputIndex = new HashMap<>(32);

    /**
     * Result to return
     */
    private final Map<String, Object> resMap = new HashMap<>(32);

    private final CountDownLatch latch = new CountDownLatch(1);

    private static final String JOB_EXECUTE_ERROR = "JobExecuteError";

    public DagRunner(String[] inputKeys, String[] outputKeys, JobInterface... dagJobs) {
        this(inputKeys, outputKeys, null, dagJobs);
    }

    public DagRunner(String[] inputKeys, String[] outputKeys, Executor executors, JobInterface... dagJobs) {
        this.executors = executors;
        this.inputKeys = inputKeys;
        this.outputKeys = Arrays.asList(outputKeys);
        this.jobCount = dagJobs.length;
        for (JobInterface dagJob : dagJobs) {
            for (String inputKey : dagJob.getJobInputKeys()) {
                inputIndex.put(inputKey, dagJob);
            }
            totalTimeout += dagJob.getJobTimeout();
        }
    }

    public Map<String, Object> runJob(Map<String, Object> input) {
        checkJob(input);
        outputStorageMap.putAll(input);
        for (String key : input.keySet()) {
            JobInterface dagJob = inputIndex.get(key);
            executeJob(dagJob);
        }
        try {
            boolean await = latch.await(totalTimeout, TimeUnit.MILLISECONDS);
            if (!await) {
                throw new InterruptedException();
            }
            if (resMap.isEmpty()) {
                throw new DagJobException("Task has end but not except output!");
            }
            return resMap;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DagJobException("Task has timeout!");
        }
    }

    public Object getOutputMapByKey(String key) {
        return outputStorageMap.get(key);
    }

    public void putAll(Map<String, Object> outputMap) {
        this.outputStorageMap.putAll(outputMap);
    }

    public void jobEnd(Map<String, Object> outputMap) {
        putAll(outputMap);
        if (isDagRunnerEnd()) {
            for (String key : outputKeys) {
                resMap.put(key, outputMap.get(key));
            }
            setTaskEnd();
            return;
        }
        if (endJobCount.incrementAndGet() == jobCount) {
            setTaskEnd();
        }
        for (String key : outputMap.keySet()) {
            JobInterface dagJob = inputIndex.get(key);
            executeJob(dagJob);
        }
    }

    private void setTaskEnd() {
        isEnd.compareAndSet(false, true);
        latch.countDown();
    }

    private boolean isJobCanBeRun(JobInterface dagJob) {
        List<String> input = dagJob.getJobInputKeys();
        boolean flag = true;
        for (String key : input) {
            if (outputStorageMap.get(key) == null) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    public boolean isDagRunnerEnd() {
        boolean flag = true;
        for (String outputKey : outputKeys) {
            if (!outputStorageMap.containsKey(outputKey)) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    public boolean isRunnerEnd() {
        return isEnd.get();
    }

    private void executeJob(JobInterface dagJob) {
        if (isRunnerEnd()) {
            return;
        }
        if (!isJobCanBeRun(dagJob)) {
            return;
        }
        List<String> jobInputKeys = dagJob.getJobInputKeys();
        Map<String, Object> jobInputMap = new HashMap<>(jobInputKeys.size() * 2);
        for (String inputKey : jobInputKeys) {
            jobInputMap.put(inputKey, outputStorageMap.get(inputKey));
        }
        Map<String, Object> jobOutputMap = new HashMap<>(8);
        asyncExecuteJob(dagJob, jobInputMap, jobOutputMap);
    }

    private void asyncExecuteJob(JobInterface dagJob, Map<String, Object> jobInputMap, Map<String, Object> jobOutputMap) {
        if (isRunnerEnd()) {
            return;
        }
        try {
            CompletableFuture<Boolean> job;
            if (executors == null) {
                job = CompletableFuture.supplyAsync(() ->
                        dagJob.notifyJob(jobInputMap, jobOutputMap));
            } else {
                job = CompletableFuture.supplyAsync(() ->
                        dagJob.notifyJob(jobInputMap, jobOutputMap), executors);
            }
            job.get(dagJob.getJobTimeout(), TimeUnit.MILLISECONDS);
            job.thenAccept(startSuccess -> {
                if (startSuccess != null && startSuccess) {
                    jobEnd(jobOutputMap);
                }
            });
        } catch (Exception e) {
            log.error("Job [{}] execute error: ", dagJob.getClass().getName(), e);
            if (isEnd.compareAndSet(false, true)) {
                resMap.put(JOB_EXECUTE_ERROR, e.getMessage());
                latch.countDown();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkJob(Map<String, Object> input) {
        checkInput(input);
    }

    private void checkInput(Map<String, Object> input) {
        for (String inputKey : this.inputKeys) {
            if (!input.containsKey(inputKey)) {
                throw new DagJobInitException(String.format("Task need key: [%s] to run!", inputKey));
            }
        }
    }
}
