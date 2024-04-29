package org.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//Learn how to write a basic producer to send data to kafka
//View basic configuration parameters
//confirm we receive the data in a Kafka Console Consumer
public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";
        //create Producer Properties
        Properties properties = new Properties();

        //connect to localhost, we can also connect to some secure cluster for ex. conduktor ui etc
        properties.setProperty("bootstrap.servers", "127.0.01:9092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true){
            log.info("polling");
            ConsumerRecords<String, String> records =   consumer.poll(Duration.ofMillis(1000));
            //this consumer.poll(Duration.ofMillis(1000)); is basically get data if its coming, when its not coming then wait till 1000 millisec

            for(ConsumerRecord<String, String> record: records){
                log.info("Key: " + record.key() + ", Value: "+ record.value());
                log.info("Partition: " + record.partition() + ", Offset: "+ record.offset());
            }
        }



    }
}
