package com.tony.hbase.config;

import com.tony.hbase.service.HelloService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author tony
 * @describe ConfigurationPropsTest
 * @date 2019-08-04
 */
//细粒度配置
@ConfigurationProperties(prefix = "service.properties")
@Data
public class ConfigurationPropsTest {
    private static final String SERVICE_NAME = "test-service";
    private String msg = SERVICE_NAME;

    @Configuration
    //如果不加该注解,idea将会报错
    @EnableConfigurationProperties(ConfigurationPropsTest.class)
    @ConditionalOnClass(HelloService.class)
    @ConditionalOnProperty(prefix = "hello", value = "enable", matchIfMissing = true)
    public class HelloServiceConfigurationAutoConfiguration {
    }

    @RestController
    public class HelloServiceConfigurationController {
        @Autowired
        private ConfigurationPropsTest helloConfiguration;

        @RequestMapping("/getObjectProperties")
        public Object getObjectProperties() {
            System.out.println(helloConfiguration.getMsg());
            return "";
        }
    }
}
