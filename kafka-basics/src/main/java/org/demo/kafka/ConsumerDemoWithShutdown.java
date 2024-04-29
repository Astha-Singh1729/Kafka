package org.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//Learn how to write a basic producer to send data to kafka
//View basic configuration parameters
//confirm we receive the data in a Kafka Console Consumer
public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
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

        //get a reference to the main thread (this is basically current thread in which my program is running)
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // when we'll run consumer.poll() then it will throw the wakeup exception

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try{
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while(true){
//                log.info("polling");
                ConsumerRecords<String, String> records =   consumer.poll(Duration.ofMillis(1000));
                //this consumer.poll(Duration.ofMillis(1000)); is basically get data if its coming, when its not coming then wait till 1000 millisec

                for(ConsumerRecord<String, String> record: records){
                    log.info("Key: " + record.key() + ", Value: "+ record.value());
                    log.info("Partition: " + record.partition() + ", Offset: "+ record.offset());
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); //close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }




    }
}
