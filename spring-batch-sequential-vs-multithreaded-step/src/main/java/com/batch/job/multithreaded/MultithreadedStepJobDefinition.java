package com.batch.job.multithreaded;

import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.TransactionVO;
import com.batch.step.ItemCountChunkListener;

@Configuration
public class MultithreadedStepJobDefinition {

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }

    @Bean(name = "multithreadedStep")
    public Step multithreadedStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  ItemReader<TransactionVO> itemReader,
                                  ItemProcessor<TransactionVO, TransactionVO> itemProcessor,
                                  FlatFileItemWriter<TransactionVO> itemWriter)
            throws Exception {

        return new StepBuilder("multithreadedStep", jobRepository)
                                                                  .<TransactionVO, TransactionVO>chunk(1000, transactionManager)
                                                                  .reader(itemReader)
                                                                  .processor(itemProcessor)
                                                                  .writer(itemWriter)
                                                                  .listener(new ItemCountChunkListener())
                                                                  .taskExecutor(taskExecutor())
                                                                  .build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener(ThreadPoolTaskExecutor taskExecutor) {
        return new JobExecutionListener() {

            @Override
            public void beforeJob(JobExecution jobExecution) {

            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                taskExecutor.shutdown();
            }
        };
    }

    @Bean
    public Job multithreadedStepJob(JobRepository jobRepository, @Qualifier("multithreadedStep") Step multithreadedStep) throws Exception {

        return new JobBuilder("multithreadedStepJob", jobRepository)
                                                                    .listener(jobExecutionListener(taskExecutor()))
                                                                    .incrementer(new RunIdIncrementer())
                                                                    .flow(multithreadedStep)
                                                                    .end()
                                                                    .build();
    }

}
