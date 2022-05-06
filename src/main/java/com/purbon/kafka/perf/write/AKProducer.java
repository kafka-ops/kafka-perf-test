package com.purbon.kafka.perf.write;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AKProducer<K, V> {

    private KafkaProducer<K, V> producer;

    public AKProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
    }

    public void write(K key, V value, String topic) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
    }

    public void write(V value, String topic) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, value);
        producer.send(record);
    }
}
