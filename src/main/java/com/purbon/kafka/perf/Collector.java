package com.purbon.kafka.perf;

import java.util.HashMap;
import java.util.Map;

public class Collector {

    private Map<String, Sensor> sensors;
    private Map<String, String> units;
    private long startTime;

    public static final String BYTES_OUT_METRIC = "bytes.out";
    public static final String BYTES_PER_SEC_UNIT = "bytes/sec";


    private static Collector instance;

    private Collector() {
        this.sensors = new HashMap<>();
        this.units = new HashMap<>();
        this.startTime = System.currentTimeMillis();
    }

    public static synchronized Collector getInstance() {
        if (instance == null) {
            instance = new Collector();
        }
        return instance;
    }


    public void register(String key, SensorGauge sensor, String unit) {
        this.sensors.put(key, sensor);
        this.units.put(key, unit);
    }

    public synchronized void collect(String key, int value) {
        sensors.get(key).append(value);
    }

    public Map<String, Sensor> get() {
        return sensors;
    }

    public String report() {
        StringBuilder sb = new StringBuilder();
        double timeDifferenceInSec = (System.currentTimeMillis()-startTime)/1000.0;
        sb.append("Execution time: "+timeDifferenceInSec + " sec\n");
        for ( var entry : sensors.entrySet() ) {
            sb.append(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue().summary()/timeDifferenceInSec);
            sb.append(" ");
            sb.append(units.get(entry.getKey()));
            sb.append("\n");
        }
        return sb.toString();
    }
}
