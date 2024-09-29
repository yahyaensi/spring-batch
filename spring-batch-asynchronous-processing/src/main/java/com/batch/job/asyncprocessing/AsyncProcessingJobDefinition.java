package com.batch.job.asyncprocessing;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.TransactionVO;
import com.batch.model.TransactionVORowMapper;

@Configuration
public class AsyncProcessingJobDefinition {

    @Bean
    public ItemReader<TransactionVO> itemReader(DataSource dataSource) throws Exception {

        return new JdbcPagingItemReaderBuilder<TransactionVO>()
                                                               .name("Reader")
                                                               .dataSource(dataSource)
                                                               .selectClause("SELECT * ")
                                                               .fromClause("FROM source_transactions ")
                                                               .whereClause("WHERE ID <= 1000000 ")
                                                               .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                                                               .pageSize(1000)
                                                               .rowMapper(new TransactionVORowMapper())
                                                               .build();
    }

    @Bean
    public ItemProcessor<TransactionVO, TransactionVO> itemProcessor() {
        return (transaction) -> {
            // Thread.sleep(1);
            return transaction;
        };
    }

    @Bean
    public AsyncItemProcessor<TransactionVO, TransactionVO> asyncItemProcessor(ItemProcessor<TransactionVO, TransactionVO> itemProcessor) {
        AsyncItemProcessor<TransactionVO, TransactionVO> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor);
        asyncItemProcessor.setTaskExecutor(taskExecutor());

        return asyncItemProcessor;
    }

    @Bean
    public FlatFileItemWriter<TransactionVO> itemWriter() {

        return new FlatFileItemWriterBuilder<TransactionVO>()
                                                             .name("Writer")
                                                             .append(false)
                                                             .resource(new FileSystemResource("transactions.txt"))
                                                             .lineAggregator(new DelimitedLineAggregator<TransactionVO>() {
                                                                 {
                                                                     setDelimiter(";");
                                                                     setFieldExtractor(new BeanWrapperFieldExtractor<TransactionVO>() {
                                                                         {
                                                                             setNames(new String[] { "id", "transactionDate", "accountId", "amount",
                                                                                     "createdAt" });
                                                                         }
                                                                     });
                                                                 }
                                                             })
                                                             .build();
    }

    @Bean
    public AsyncItemWriter<TransactionVO> asyncItemWriter(FlatFileItemWriter<TransactionVO> itemWriter) {
        AsyncItemWriter<TransactionVO> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter);
        return asyncItemWriter;
    }

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

    @Bean(name = "asyncProcessingStep")
    public Step asyncProcessingStep(JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager,
                                    ItemReader<TransactionVO> itemReader,
                                    AsyncItemProcessor<TransactionVO, TransactionVO> asyncItemProcessor,
                                    AsyncItemWriter<TransactionVO> asyncItemWriter)
            throws Exception {

        return new StepBuilder("asyncProcessingStep", jobRepository)
                                                                    .<TransactionVO, Future<TransactionVO>>chunk(1000, transactionManager)
                                                                    .reader(itemReader)
                                                                    .processor(asyncItemProcessor)
                                                                    .writer(asyncItemWriter)
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
    public Job asyncProcessingStepJob(JobRepository jobRepository, @Qualifier("asyncProcessingStep") Step asyncProcessingStep) throws Exception {

        return new JobBuilder("asyncProcessingStepJob", jobRepository)
                                                                      .listener(jobExecutionListener(taskExecutor()))
                                                                      .incrementer(new RunIdIncrementer())
                                                                      .flow(asyncProcessingStep)
                                                                      .end()
                                                                      .build();
    }

}
