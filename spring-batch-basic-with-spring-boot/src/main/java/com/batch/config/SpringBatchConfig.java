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
import com.batch.service.CustomItemProcessor;
import com.batch.service.RecordFieldSetMapper;

@Configuration
public class SpringBatchConfig {

    @Value("input/record.csv")
    private Resource inputCsv;

    @Value("file:xml/output.xml")
    private WritableResource outputXml;

    public ItemReader<Transaction> itemReader(Resource inputData) throws UnexpectedInputException {

        String[] tokens = { "username", "userid", "transactiondate", "amount" };
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(tokens);
        tokenizer.setDelimiter(",");

        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());

        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();
        reader.setResource(inputData);
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public ItemProcessor<Transaction, Transaction> itemProcessor() {
        return new CustomItemProcessor();
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
    protected Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                         @Qualifier("itemProcessor") ItemProcessor<Transaction, Transaction> processor,
                         ItemWriter<Transaction> writer) {
        return new StepBuilder("step1", jobRepository)
                                                      .<Transaction, Transaction>chunk(10, transactionManager)
                                                      .reader(itemReader(inputCsv))
                                                      .processor(processor)
                                                      .writer(writer)
                                                      .build();
    }

    @Bean(name = "basicBatchJob")
    public Job job(JobRepository jobRepository, @Qualifier("step1") Step step1) {
        return new JobBuilder("basicBatchJob", jobRepository).preventRestart().start(step1).build();
    }

    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        return builder.setType(EmbeddedDatabaseType.H2)
                      .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                      .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                      .build();
    }

    /**
     * Starting with spring-boot 3.0, @EnableBatchProcessing annotation is discouraged. We declare manually JobRepository,
     * JobLauncher and TransactionManager beans. In addition, JobBuilderFactory and StepBuilderFactory are deprecated,
     * and it’s recommended to use JobBuilder and StepBuilder class with the name of the job/step builder.
     */

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
