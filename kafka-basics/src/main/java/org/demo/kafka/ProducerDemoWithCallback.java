package org.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//Learn how to write a basic producer to send data to kafka
//View basic configuration parameters
//confirm we receive the data in a Kafka Console Consumer
public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer");

        //create Producer Properties
        Properties properties = new Properties();

        //connect to localhost, we can also connect to some secure cluster for ex. conduktor ui etc
        properties.setProperty("bootstrap.servers", "127.0.01:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");   never go for smaller batch size in production, keep kafka default settings
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //I can add <String, String> or just make this <> as shown above inside new KafkaProducer, this is some explicit argument and implicit argument logic


        for(int j=0; j<10;j++){
            for(int i=0; i<30;i++){
                //create a Producer Record
                ProducerRecord<String, String> producerRecord=  new ProducerRecord<>("demo_java", j + "message number = " + i + " via java programm with callback in a loop" );

                //send data --asynchronous operation
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executed every time a record successfully sent or an exception is thrown
                        if(e == null){
                            //the record was succefully sent
                            log.info("Received new metadata \n" + "Topic: " + metadata.topic() + "\n"
                                    + "Partition: " + metadata.partition() + "\n"
                                    + "Offset: " + metadata.offset() + "\n"
                                    + "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }

                    }
                });
            }
        }






        //tell the producer to send all data and block until done --synchronous operation
        producer.flush();

        //flush and close the producer
        producer.close();
        //this producer.close() also calls producer.flush() first, we have written it in code just to knnow that it also exist independently
        //smjh ni aaya reason for these two exist, course mai aisa kuch bola cuz senddata == async so if these two lines dont exist program can close without sending the data to kafka



    }
}
