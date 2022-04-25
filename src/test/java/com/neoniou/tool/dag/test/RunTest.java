package com.neoniou.tool.dag.test;

import com.neoniou.tool.dag.DagRunner;
import com.neoniou.tool.dag.command.job.FunctionJobCommand;
import com.neoniou.tool.dag.command.JobInterface;
import com.neoniou.tool.dag.exception.DagJobException;
import com.neoniou.tool.dag.exception.DagJobInitException;
import com.neoniou.tool.dag.test.job.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Neo.Zzj
 * @date 2021/12/30
 */
class RunTest {

    @Test
    void simpleTest1() {
        JobInterface dagJobA = new JobA();
        JobInterface dagJobB = new JobB();
        JobInterface dagJobC = new JobC();

        DagRunner dagRunner = new DagRunner(
                new String[]{"x", "y", "a"},
                new String[]{"u", "v"},
                dagJobA, dagJobB, dagJobC);

        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "i-x");
        inputMap.put("y", "i-y");
        inputMap.put("a", "i-a");
        Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        System.out.println(resultMap);
    }

    @Test
    void fucJobTest() {
        JobInterface dagJobA = new FunctionJobCommand((inputMap, outputMap) -> {
            System.out.printf("%s execute, inputMap: %s%n", this.getClass().getSimpleName(), inputMap.toString());
            outputMap.put("z", "jobA-z");
            outputMap.put("w", "jobA-w");
        }, new String[]{"x", "y"}, new String[]{"z", "w"}, 100L);
        JobInterface dagJobB = new JobB();
        JobInterface dagJobC = new JobC();

        DagRunner dagRunner = new DagRunner(
                new String[]{"x", "y", "a"},
                new String[]{"u", "v"},
                dagJobA, dagJobB, dagJobC);

        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "i-x");
        inputMap.put("y", "i-y");
        inputMap.put("a", "i-a");
        Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        System.out.println(resultMap);
    }

    @Test
    void noOutputTest() {
        JobInterface dagJobA = new JobA();
        JobInterface dagJobB = new JobB();
        //Wrong output
        JobInterface dagJobC = new JobC2WrongOutput();

        DagRunner dagRunner = new DagRunner(
                new String[]{"x", "y", "a"},
                new String[]{"u", "v"},
                dagJobA, dagJobB, dagJobC);

        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "i-x");
        inputMap.put("y", "i-y");
        inputMap.put("a", "i-a");

        DagJobException dagJobException = Assertions.assertThrows(DagJobException.class, () -> {
            Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        });
        System.out.println("Error: " + dagJobException.getMessage());
    }

    @Test
    void inputErrorTest() {
        JobInterface dagJobA = new JobA();
        JobInterface dagJobB = new JobB();
        JobInterface dagJobC = new JobC();

        DagRunner dagRunner = new DagRunner(
                new String[]{"x", "y", "a"},
                new String[]{"u", "v"},
                dagJobA, dagJobB, dagJobC);

        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "i-x");
        inputMap.put("y", "i-y");
        //inputMap.put("a", "i-a");

        DagJobInitException dagJobInitException = Assertions.assertThrows(DagJobInitException.class, () -> {
            Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        });
        System.out.println("Error: " + dagJobInitException.getMessage());
    }

    @Test
    void timeoutTest() {
        JobInterface dagJobA = new JobA();
        //Timeout job
        JobInterface dagJobB = new JobBTimeout();
        JobInterface dagJobC = new JobC2WrongOutput();

        DagRunner dagRunner = new DagRunner(
                new String[]{"x", "y", "a"},
                new String[]{"u", "v"},
                dagJobA, dagJobB, dagJobC);

        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "i-x");
        inputMap.put("y", "i-y");
        inputMap.put("a", "i-a");

        try {
            Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
