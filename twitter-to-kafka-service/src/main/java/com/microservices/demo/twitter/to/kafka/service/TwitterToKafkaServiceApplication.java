package com.microservices.demo.twitter.to.kafka.service;

import java.util.Arrays;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.impl.TwitterKafkaStreamRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);    
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts...");
        String msg = Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {}));
        LOG.info(msg);
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
