package com.batch.config;

import javax.sql.DataSource;

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
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.Transaction;
import com.batch.service.CustomSkipPolicy;
import com.batch.service.RecordFieldSetMapper;
import com.batch.service.SkippingItemProcessor;
import com.batch.service.exception.MissingUsernameException;
import com.batch.service.exception.NegativeAmountException;

@Configuration
public class SpringBatchConfig {

    @Value("input/recordWithInvalidData.csv")
    private Resource invalidInputCsv;

    @Value("file:xml/output.xml")
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
        reader.setResource(invalidInputCsv);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public ItemProcessor<Transaction, Transaction> itemProcessor() {
        return new SkippingItemProcessor();
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
    public Step skippingStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             ItemReader<Transaction> reader,
                             ItemProcessor<Transaction, Transaction> processor,
                             ItemWriter<Transaction> writer) {
        return new StepBuilder("skippingStep", jobRepository)
                                                             .<Transaction, Transaction>chunk(10, transactionManager)
                                                             .reader(reader)
                                                             .processor(processor)
                                                             .writer(writer)
                                                             .faultTolerant()
                                                             .skipLimit(2)
                                                             .skip(MissingUsernameException.class)
                                                             .skip(NegativeAmountException.class)
                                                             .build();
    }

    @Bean(name = "skippingBatchJob")
    public Job skippingJob(JobRepository jobRepository, @Qualifier("skippingStep") Step skippingStep) {
        return new JobBuilder("skippingBatchJob", jobRepository)
                                                                .start(skippingStep)
                                                                .preventRestart()
                                                                .build();
    }

    @Bean
    public Step skipPolicyStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                               ItemReader<Transaction> reader,
                               ItemProcessor<Transaction, Transaction> processor,
                               ItemWriter<Transaction> writer) {
        return new StepBuilder("skipPolicyStep", jobRepository)
                                                               .<Transaction, Transaction>chunk(10, transactionManager)
                                                               .reader(reader)
                                                               .processor(processor)
                                                               .writer(writer)
                                                               .faultTolerant()
                                                               .skipPolicy(new CustomSkipPolicy())
                                                               .build();
    }

    @Bean(name = "skipPolicyBatchJob")
    public Job skipPolicyBatchJob(JobRepository jobRepository, @Qualifier("skipPolicyStep") Step skipPolicyStep) {
        return new JobBuilder("skipPolicyBatchJob", jobRepository)
                                                                  .start(skipPolicyStep)
                                                                  .preventRestart()
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
