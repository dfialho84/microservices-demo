package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
@Slf4j
public class MockKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData config;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[] {
        "Lorem ipsum dolor",
        "amet mustache knausgaard",
        "+1, blue bottle waistcoat",
        "tbh semiotics artisan synth",
        "stumptown gastropub cornhole",
        "celiac swag. Brunch raclette vexillologist",
        "post-ironic glossier ennui XOXO mlkshk",
        "godard pour-over blog tumblr humblebrag.",
        "Blue bottle put a bird on it twee prism",
        "biodiesel brooklyn. Blue bottle ennui tbh succulents."
    };

    private static final String tweetAsRawJson = "{ \"created_at\": \"{0}\", \"id\": \"{1}\", \"text\": \"{2}\", \"user\": { \"id\": \"{3}\" } }";
    private static final String TWITTER_SATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData config, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.config = config;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    private String constructRandomTweet(String [] keywords, int tweetLength) {
        StringBuilder tweet = new StringBuilder();
        for(int i = 0; i < tweetLength; i++) {
            int index = RANDOM.nextInt(WORDS.length);
            String text = WORDS[index];
            tweet.append(text).append(" ");
            if(i == tweetLength / 2) {
                index = RANDOM.nextInt(keywords.length);
                text = keywords[index];
                tweet.append(text).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    private String getRandomTweetContent(String [] keywords, int min, int max) {
        int tweetLength = RANDOM.nextInt(max - min + 1) + min;
        return constructRandomTweet(keywords, tweetLength);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getFormattedTweet(String [] keywords, int min, int max) {
        String [] params = new String[] {
            ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_SATUS_DATE_FORMAT, Locale.ENGLISH)),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
            getRandomTweetContent(keywords, min, max),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        return formatTweetAsJsonWithParams(params);
    }

    private void sleep(long milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!", e);
        }
    }

    private void simulateTwitterStream(String [] keywords, int min, int max, long sleepTime) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while(true) {
                    String formattedTweet = getFormattedTweet(keywords, min, max);
                    Status status = TwitterObjectFactory.createStatus(formattedTweet);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTime);
                }

            } catch(TwitterException e) {
                log.error("Error creating twiiter status", e);
            }            
        });        
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = config.getTwitterKeywords().toArray(new String[0]);
        int minLength = config.getMockMinTweetLength();
        int maxLength = config.getMockMaxTweetLength();
        long sleepTime = config.getMockSleepMs();
        log.info("Starting mock filtering twitter for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minLength, maxLength, sleepTime);
    }
    
}
