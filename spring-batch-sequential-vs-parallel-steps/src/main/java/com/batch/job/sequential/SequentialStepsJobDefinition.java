package com.batch.job.sequential;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SequentialStepsJobDefinition {

    @Bean
    public Job sequentialStepsJob(JobRepository jobRepository,
                                  @Qualifier("transactionStep") Step transactionStep,
                                  @Qualifier("accountStep") Step accountStep,
                                  @Qualifier("aggregationStep") Step aggregationStep) {

        return new JobBuilder("sequentialStepsJob", jobRepository)
                                                                  .incrementer(new RunIdIncrementer())
                                                                  .flow(transactionStep)
                                                                  .next(accountStep)
                                                                  .next(aggregationStep)
                                                                  .end()
                                                                  .build();
    }
}
