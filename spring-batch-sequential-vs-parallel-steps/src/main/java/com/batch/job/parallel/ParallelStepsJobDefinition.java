package com.batch.job.parallel;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class ParallelStepsJobDefinition {

    @Bean
    public Job parallelStepsJob(JobRepository jobRepository,
                                @Qualifier("transactionStep") Step transactionStep,
                                @Qualifier("accountStep") Step accountStep,
                                @Qualifier("aggregationStep") Step aggregationStep) {

        Flow transactionFlow = new FlowBuilder<Flow>("transactionFlow")
                                                                       .from(transactionStep)
                                                                       .end();

        Flow accountFlow = new FlowBuilder<Flow>("accountFlow")
                                                               .from(accountStep)
                                                               .end();

        Flow parallelFlows = new FlowBuilder<Flow>("parallelFlows")
                                                                   .split(new SimpleAsyncTaskExecutor())
                                                                   .add(transactionFlow, accountFlow)
                                                                   .build();

        return new JobBuilder("parallelStepsJob", jobRepository)
                                                                .incrementer(new RunIdIncrementer())
                                                                .start(parallelFlows)
                                                                .next(aggregationStep)
                                                                .end()
                                                                .build();
    }
}
