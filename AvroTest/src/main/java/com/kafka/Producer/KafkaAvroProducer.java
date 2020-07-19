package com.kafka.Producer;

import com.avroTest.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaAvroProducer {
    
    public static void main(String[] args) throws Exception {
        
        User[] users = new User[100];
        for(int i = 0; i < 100; i++) {
            users[i] = new User();
            users[i].setName("小明"+i);
            users[i].setFavoriteColor("红色"+i);
            users[i].setFavoriteNumber(i);
        }
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置序列化类为自定义的 avro 序列化类
        props.put("value.serializer", "com.avro.serializer.AvroSerializer");

        Producer<String, User> producer = new KafkaProducer<>(props);
        
        for(User user : users) {
            ProducerRecord<String, User> record = new ProducerRecord<>("flink1", user);
            RecordMetadata metadata = producer.send(record).get();
            StringBuilder sb = new StringBuilder();
            sb.append("user: ").append(user.toString()).append(" has been sent successfully!").append("\n")
                .append("send to partition ").append(metadata.partition())
                .append(", offset = ").append(metadata.offset());
            System.out.println(sb.toString());
            Thread.sleep(3000);
        }
        
        producer.close();
    }
}
