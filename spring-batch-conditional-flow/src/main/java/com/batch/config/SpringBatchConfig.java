package com.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class SpringBatchConfig {

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_one", jobRepository)
                                                         .tasklet(new Tasklet() {
                                                             @Override
                                                             public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                                                                     throws Exception {
                                                                 log.info("STEP1 EXECUTED");
                                                                 return RepeatStatus.FINISHED;
                                                             }
                                                         }, transactionManager)
                                                         .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {

        return new StepBuilder("step_two", jobRepository)
                                                         .tasklet(new Tasklet() {
                                                             @Override
                                                             public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                                                                     throws Exception {
                                                                 log.info("STEP2 EXECUTED");
                                                                 boolean isTrue = true;
                                                                 if (isTrue) {
                                                                     throw new Exception("Exception occured!!");
                                                                 }
                                                                 return RepeatStatus.FINISHED;
                                                             }
                                                         }, transactionManager)
                                                         .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_three", jobRepository)
                                                           .tasklet(new Tasklet() {
                                                               @Override
                                                               public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
                                                                   log.info("STEP3 EXECUTED");
                                                                   return RepeatStatus.FINISHED;
                                                               }
                                                           }, transactionManager)
                                                           .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_four", jobRepository)
                                                          .tasklet(new Tasklet() {
                                                              @Override
                                                              public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
                                                                  log.info("STEP4 EXECUTED");
                                                                  return RepeatStatus.FINISHED;
                                                              }
                                                          }, transactionManager)
                                                          .build();
    }

    @Bean
    public Job conditionalStepsJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
        return new JobBuilder("conditionalStepsJob", jobRepository)
                                                                   .start(step1(jobRepository, transactionManager))
                                                                   .on(ExitStatus.COMPLETED.getExitCode())
                                                                   .to(step2(jobRepository, transactionManager))
                                                                   .from(step2(jobRepository, transactionManager))
                                                                   .on(ExitStatus.COMPLETED.getExitCode())
                                                                   .to(step3(jobRepository, transactionManager))
                                                                   .from(step2(jobRepository, transactionManager))
                                                                   .on(ExitStatus.FAILED.getExitCode())
                                                                   .to(step4(jobRepository, transactionManager))
                                                                   .end()
                                                                   .build();
    }

    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        return builder.setType(EmbeddedDatabaseType.H2)
                      .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                      .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                      .build();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobRepository jobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource());
        factory.setTransactionManager(transactionManager());
        // JobRepositoryFactoryBean's methods Throws Generic Exception,
        // it would have been better to have a specific one
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        // TaskExecutorJobLauncher's methods Throws Generic Exception,
        // it would have been better to have a specific one
        jobLauncher.setJobRepository(jobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}
