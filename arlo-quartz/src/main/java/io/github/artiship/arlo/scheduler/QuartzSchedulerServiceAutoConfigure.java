package io.github.artiship.arlo.scheduler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(QuartzSchedulerService.class)
@EnableConfigurationProperties(QuartzConfig.class)
public class QuartzSchedulerServiceAutoConfigure {

    @Autowired
    private QuartzConfig quartzConfig;

    @Bean
    @ConditionalOnMissingBean
    public QuartzSchedulerService quartzSchedulerService() {
        return new QuartzSchedulerService(quartzConfig);
    }
}
