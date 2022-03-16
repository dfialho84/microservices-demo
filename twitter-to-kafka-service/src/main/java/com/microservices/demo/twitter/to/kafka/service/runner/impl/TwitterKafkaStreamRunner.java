package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import java.util.Arrays;

import javax.annotation.PreDestroy;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData config;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;


    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData config, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.config = config;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }  

    private void addFilter() {
        String[] keywords = config.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutdown() {
        if(twitterStream != null) {
            LOG.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();        
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }
    
}
