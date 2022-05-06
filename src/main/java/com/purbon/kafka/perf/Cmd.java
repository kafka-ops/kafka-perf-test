package com.purbon.kafka.perf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Cmd {

    public static Properties config(String fileParam) {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream(fileParam));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }


    public static void main(String[] args) {

        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();

        String fileParam = args[0];
        KafkaPerfTool kpf = new KafkaPerfTool(5);

        executor.submit(() -> {
            Properties props = config(fileParam);
            kpf.run(props);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           kpf.stop();
        }));
    }
}
