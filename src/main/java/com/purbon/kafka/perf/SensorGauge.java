package com.purbon.kafka.perf;

public class SensorGauge implements Sensor {

    private int value;

    public SensorGauge() {
        this.value = -1;
    }

    public void append(int sample) {
        if (value == -1) {
            this.value = sample;
        } else {
            this.value += sample;
        }
    }

    public int summary() {
        return value;
    }
}
