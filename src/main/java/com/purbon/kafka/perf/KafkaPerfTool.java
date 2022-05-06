package com.purbon.kafka.perf;

import com.purbon.kafka.perf.write.WriterAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.purbon.kafka.perf.Collector.BYTES_OUT_METRIC;
import static com.purbon.kafka.perf.Collector.BYTES_PER_SEC_UNIT;

public class KafkaPerfTool {

    private static final Logger log = LogManager.getLogger(KafkaPerfTool.class);


    private ThreadPoolExecutor service;
    private List<WriterAction> actions;

    private int numThreads;

    public static final String WRITE_TOPIC = "my.write.topic";

    public KafkaPerfTool(int numThreads) {
        this.numThreads = numThreads;
        service = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
        actions = new ArrayList<>();
    }

    public void run(Properties config) {

        Collector collector = Collector.getInstance();
        collector.register(BYTES_OUT_METRIC, new SensorGauge(), BYTES_PER_SEC_UNIT);

        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

        for (int i=0; i < numThreads; i++) {
            actions.add(new WriterAction(queue, config, collector));
        }
        for(WriterAction action : actions) {
           service.submit(action);
        }

        for(; true; ) {
            String msg = generate();
            queue.add(msg);
        }
    }

    public void stop() {
        log.debug("Stopping the KafkaPerfTool");
        for ( WriterAction action : actions ) {
            action.stop();
        }
        log.debug("shutting down....");
        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                service.shutdownNow();
            }
        } catch (InterruptedException ex) {
            service.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.debug("stop finished");
        System.out.println(Collector.getInstance().report());
    }

    private String generate() {
        byte[] array = new byte[1024]; // 512kb
        new Random().nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}
