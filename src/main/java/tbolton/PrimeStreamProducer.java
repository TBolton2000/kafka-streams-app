package tbolton;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import tbolton.PrimeData;

public class PrimeStreamProducer {
    
    public static void main(String[] args) throws Exception {
        String topicName = "numbers-input";
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        
        //Specify buffer size in config
        props.put("batch.size", 16384);
        
        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);
        
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 16777216); // 16 MB
        
        props.put("key.serializer", 
            "org.apache.kafka.common.serialization.StringSerializer");
            
        props.put("value.serializer", 
            "org.apache.kafka.streams.JsonPOJOSerializer");
        
        Producer<String, PrimeData> producer = new KafkaProducer
            <String, PrimeData>(props);
                
        for (Long num = 2L; num < 10000L; num++)
        {
            PrimeData data = new PrimeData(UUID.randomUUID(), System.nanoTime(), num);
            producer.send(new ProducerRecord<String, PrimeData>(topicName, 
                data.getUuid().toString(), data));
        }
        System.out.println("Message sent successfully");
        producer.close();

    }
}
