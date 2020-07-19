package com.kafka.Consumer;

import com.avroTest.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {
    
    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");
        props.put("group.id", "flink1");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 设置反序列化类为自定义的avro反序列化类
        props.put("value.deserializer","com.avro.deserializer.AvroDeserializer");
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        
        consumer.subscribe(Collections.singletonList("flink1"));
        
        try {
            while(true) {
                ConsumerRecords<String, User> records = consumer.poll(100);
                for(ConsumerRecord<String, User> record : records) {
                    User user = record.value();
                    System.out.println(user.toString());
                }
            }
        }finally {
            consumer.close();
        }
    }
}