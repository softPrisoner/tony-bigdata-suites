package com.tony.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author tony
 * @describe TonyTransactionTestApplication
 * @date 2019-08-03
 */
@SpringBootApplication
@EnableConfigurationProperties
public class TonyTransactionTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(TonyTransactionTestApplication.class, args);
    }
}
