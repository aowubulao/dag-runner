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
    void fucJobTest2() {
        // 定义一个jobA，依赖的输入是x，输出是z,w，超时时间100ms
        JobInterface dagJobA = new FunctionJobCommand((inputMap, outputMap) -> {
            System.out.printf("a execute, inputMap: %s%n", inputMap.toString());
            outputMap.put("z", "jobA-out-z");
            outputMap.put("w", "jobA-out-w");
        }, new String[]{"x"}, new String[]{"z", "w"}, 100L);

        // 定义一个jobB，依赖的输入是z，输出是j，超时时间100ms
        JobInterface dagJobB = new FunctionJobCommand((inputMap, outputMap) -> {
            System.out.printf("b execute, inputMap: %s%n", inputMap.toString());
            outputMap.put("j", "jobB-out-j");
        }, new String[]{"z"}, new String[]{"j"}, 100L);

        // 定义一个jobC，依赖的输入是w，输出是k，超时时间100ms
        JobInterface dagJobC = new FunctionJobCommand((inputMap, outputMap) -> {
            System.out.printf("c execute, inputMap: %s%n", inputMap.toString());
            outputMap.put("k", "jobC-out-k");
        }, new String[]{"w"}, new String[]{"j"}, 100L);

        // 定义一个jobD，依赖的输入是j,k，输出是r，超时时间100ms
        JobInterface dagJobD = new FunctionJobCommand((inputMap, outputMap) -> {
            System.out.printf("d execute, inputMap: %s%n", inputMap.toString());
            outputMap.put("r", "jobD-out-r");
        }, new String[]{"j", "k"}, new String[]{"r"}, 100L);

        // 执行
        Map<String, Object> inputMap = new HashMap<>(8);
        inputMap.put("x", "init-input-x");

        DagRunner dagRunner = new DagRunner(
                new String[]{"x"},
                new String[]{"r"},
                dagJobA, dagJobB, dagJobC, dagJobD);
        Map<String, Object> resultMap = dagRunner.runJob(inputMap);
        Assertions.assertNotNull(resultMap);
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
