package com.gunjan.tweet.kafka.producer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class WriteTweetToKafka
{
    
    public static void main(String[] args)
    {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("")
                .setOAuthConsumerSecret("")
                .setOAuthAccessToken("")
                .setOAuthAccessTokenSecret("");
        
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        
        Producer<String,String> producer = new KafkaProducer<>(getKafkaProperties());
        twitterStream.addListener(new StatusListener()
        {
            AtomicInteger tweetCount = new AtomicInteger(0);
            
            @Override
            public void onStatus(Status status)
            {
                try
                {
                    
                    //System.out.println(tweetCount.getAndIncrement() + "=> " + status.getText());
                    //root@k8s-master:/home/ubuntu/TweetCount/kafka_2.11-2.3.0/bin# ./kafka-topics.sh --create --bootstrap-server 10.71.69.236:31117 --replication-factor 3 --partitions 6
                    // --config min.insync.replicas=3 --topic tweeter-topic
                    Callback callback = new Callback()
                    {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception)
                        {
                            System.out.println(metadata);
                            exception.printStackTrace();
                        }
                    };
                    producer.send(new ProducerRecord("tweeter-topic", new ObjectMapper().writeValueAsString(status)), callback);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
            {
            }
            
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses)
            {
                System.out.println("onTrackLimitationNotice = " + numberOfLimitedStatuses);
            }
            
            @Override
            public void onScrubGeo(long userId, long upToStatusId)
            {
            }
            
            @Override
            public void onStallWarning(StallWarning warning)
            {
            }
            
            @Override
            public void onException(Exception ex)
            {
                ex.printStackTrace();
            }
        });
        
        twitterStream.sample();
        System.out.println("Message sent successfully");
    }
    
    private static Properties getKafkaProperties()
    {
        Properties props = new Properties();
        
        //Assign localhost id
        props.put("bootstrap.servers", "10.71.69.236:31117,10.71.69.236:31118,10.71.69.236:31119");
        
        //Set acknowledgements for producer requests.
        props.put("acks", "-1");
        
        props.put("enable.idempotence", true);
        
        //If the request fails, the producer can automatically retry,
        props.put("retries", 10);
        
        //Specify buffer size in config
        props.put("batch.size", 32768);
        
        //Reduce the no of requests less than 0
        props.put("linger.ms", 20);
        
        props.put("compression.type", "snappy");
        
        //If the producer produces faster than the broker can take, the records will be buffered in memory.
        // Below is the size of the send buffer.
        props.put("buffer.memory", 33554432); // 32 MB
        
        // If send buffer is full, producer will block for 60000 ms before throwing exception
        props.put("max.block.ms", 60000);
        
        props.put("key.serializer", StringSerializer.class.getName());
        
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }
}