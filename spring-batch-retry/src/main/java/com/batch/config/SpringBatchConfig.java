package com.batch.config;

import javax.sql.DataSource;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.Transaction;
import com.batch.service.RecordFieldSetMapper;
import com.batch.service.RetryItemProcessor;

@Configuration
public class SpringBatchConfig {

    private static final int TWO_SECONDS = 2000;

    @Value("input/recordRetry.csv")
    private Resource inputCsv;

    @Value("file:xml/retryOutput.xml")
    private WritableResource outputXml;

    @Bean
    public ItemReader<Transaction> itemReader() throws UnexpectedInputException {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();

        String[] tokens = { "username", "userid", "transactiondate", "amount" };
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(tokens);
        tokenizer.setDelimiter(",");

        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());

        reader.setLineMapper(lineMapper);
        reader.setResource(inputCsv);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public ItemProcessor<Transaction, Transaction> itemProcessor() {
        return new RetryItemProcessor();
    }

    @Bean
    public ItemWriter<Transaction> itemWriter(Marshaller marshaller) {
        StaxEventItemWriter<Transaction> itemWriter = new StaxEventItemWriter<>();
        itemWriter.setMarshaller(marshaller);
        itemWriter.setRootTagName("transactions");
        itemWriter.setResource(outputXml);
        return itemWriter;
    }

    @Bean
    public Marshaller marshaller() {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Transaction.class);
        return marshaller;
    }

    @Bean
    public Step retryStep(JobRepository jobRepository,
                          PlatformTransactionManager transactionManager,
                          ItemReader<Transaction> reader,
                          ItemProcessor<Transaction, Transaction> processor,
                          ItemWriter<Transaction> writer) {
        return new StepBuilder("retryStep", jobRepository)
                                                          .<Transaction, Transaction>chunk(10, transactionManager)
                                                          .reader(reader)
                                                          .processor(processor)
                                                          .writer(writer)
                                                          .faultTolerant()
                                                          .retryLimit(3)
                                                          .retry(ConnectTimeoutException.class)
                                                          .retry(DeadlockLoserDataAccessException.class)
                                                          .build();
    }

    @Bean(name = "retryBatchJob")
    public Job retryJob(JobRepository jobRepository, @Qualifier("retryStep") Step retryStep) {
        return new JobBuilder("retryBatchJob", jobRepository)
                                                             .start(retryStep)
                                                             .build();
    }

    @Bean
    public CloseableHttpClient closeableHttpClient() {
        final RequestConfig config = RequestConfig.custom()
                                                  .setConnectTimeout(TWO_SECONDS)
                                                  .build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
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
