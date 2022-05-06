package com.purbon.kafka.perf;

public interface Sensor {
    void append(int sample);
    int summary();
}
