package com.gunjan.tweet.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
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
            @Override
            public void onStatus(Status status)
            {
                try
                {
                    System.out.println(status);
                    producer.send(new ProducerRecord<>("admintome-test", new ObjectMapper().writeValueAsString(status)));
                }
                catch(JsonProcessingException e)
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
                System.out.println("onTrackLimitationNotice = " +  numberOfLimitedStatuses);
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
        System.out.println("Message sent successfully Gunjan");
    }
    
    private static Properties getKafkaProperties()
    {
        Properties props = new Properties();
        
        //Assign localhost id
        props.put("bootstrap.servers", "10.71.69.236:31440");
        
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        
        //Specify buffer size in config
        props.put("batch.size", 16384);
        
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}