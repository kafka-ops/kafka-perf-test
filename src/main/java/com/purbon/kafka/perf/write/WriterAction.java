package com.purbon.kafka.perf.write;

import com.purbon.kafka.perf.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.purbon.kafka.perf.KafkaPerfTool.WRITE_TOPIC;

public class WriterAction implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(WriterAction.class);

    private final ConcurrentLinkedQueue<String> queue;
    private final Collector collector;
    private AtomicBoolean running;
    private Properties config;

    public WriterAction(ConcurrentLinkedQueue<String> queue, Properties config, Collector collector) {
        this.queue = queue;
        this.config = config;
        this.collector = collector;
    }

    @Override
    public void run() {
        this.running = new AtomicBoolean(true);
        var producer = new AKProducer<String, String>(config);
        while(shouldBeRunning()) {
            String elem = queue.poll();
            if (elem != null) {
                measureAndCollect(elem);
                producer.write(elem, WRITE_TOPIC);
            }
        }
        LOGGER.debug("Thread finished");
    }

    private void measureAndCollect(String elem) {
        var length = elem.getBytes(StandardCharsets.UTF_8).length;
        collector.collect(Collector.BYTES_OUT_METRIC, length);
    }

    private boolean shouldBeRunning() {
        return running.get();
    }

    public void stop() {
        this.running.set(false);
    }
}
