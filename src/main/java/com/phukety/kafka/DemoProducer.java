package com.phukety.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class DemoProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.177.130:9092");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id");

        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("words", "key" + i, "value" + i));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // 此处的三个异常由于不能回滚,所以只能单独处理并且及时关闭producer
            // ProducerFencedException 当有另外一个生产者以相同的transactional_id启动,则抛出该异常
            // OutOfOrderSequenceException 当broker从生产者中接收到了意外的序列号,很可能是出现了数据丢失.此时事务生产者会抛出该异常
            // AuthorizationException kafka权限问题
            producer.close();
        } catch (KafkaException e) {
            // 其余异常直接回滚
            producer.abortTransaction();
        }
    }
}
