package com.batch.config;

import java.io.IOException;
import java.text.ParseException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.Transaction;
import com.batch.service.RecordFieldSetMapper;

@Configuration
public class SpringBatchConfig {

    @Autowired
    private ResourcePatternResolver resourcePatternResolver;

    @Bean(name = "partitionerJob")
    public Job partitionerJob(JobRepository jobRepository, PlatformTransactionManager transactionManager)
            throws UnexpectedInputException, ParseException {
        return new JobBuilder("partitionerJob", jobRepository)
                                                              .listener(jobExecutionListener(taskExecutor()))
                                                              .start(partitionStep(jobRepository, transactionManager))
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
    public Step partitionStep(JobRepository jobRepository, PlatformTransactionManager transactionManager)
            throws UnexpectedInputException, ParseException {
        return new StepBuilder("partitionStep", jobRepository)
                                                              .partitioner("slaveStep", partitioner())
                                                              .step(slaveStep(jobRepository, transactionManager))
                                                              .taskExecutor(taskExecutor())
                                                              .build();
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager)
            throws UnexpectedInputException, ParseException {
        return new StepBuilder("slaveStep", jobRepository)
                                                          .<Transaction, Transaction>chunk(1, transactionManager)
                                                          .reader(itemReader(null))
                                                          .writer(itemWriter(marshaller(), null))
                                                          .build();
    }

    @Bean
    public CustomMultiResourcePartitioner partitioner() {
        CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
        Resource[] resources;
        try {
            resources = resourcePatternResolver.getResources("file:src/main/resources/input/partitioner/*.csv");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
        }
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> itemReader(@Value("#{stepExecutionContext[fileName]}") String filename)
            throws UnexpectedInputException, ParseException {
        String[] tokens = { "username", "userid", "transactiondate", "amount" };
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(tokens);
        tokenizer.setDelimiter(",");

        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());

        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("input/partitioner/" + filename));
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean(destroyMethod = "")
    @StepScope
    public StaxEventItemWriter<Transaction> itemWriter(Marshaller marshaller, @Value("#{stepExecutionContext[opFileName]}") String filename) {
        StaxEventItemWriter<Transaction> itemWriter = new StaxEventItemWriter<>();
        itemWriter.setMarshaller(marshaller);
        itemWriter.setRootTagName("transactions");
        itemWriter.setResource(new FileSystemResource("src/main/resources/output/" + filename));
        return itemWriter;
    }

    @Bean
    public Marshaller marshaller() {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Transaction.class);
        return marshaller;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setQueueCapacity(5);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
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
