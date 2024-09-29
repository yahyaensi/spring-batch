package com.batch.step;

import java.util.Collections;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.SourceTransactionVO;
import com.batch.model.SourceTransactionVORowMapper;

@Configuration
public class TransactionsStepDefinition {

    @Bean(name = "transactionStep")
    public Step transactionStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                ItemReader<SourceTransactionVO> transactionReader,
                                ItemProcessor<SourceTransactionVO, SourceTransactionVO> transactionProcessor,
                                JdbcBatchItemWriter<SourceTransactionVO> transactionWriter)
            throws Exception {
        return new StepBuilder("transactionStep", jobRepository)
                                                                .<SourceTransactionVO, SourceTransactionVO>chunk(1000, transactionManager)
                                                                .reader(transactionReader)
                                                                .processor(transactionProcessor)
                                                                .writer(transactionWriter)
                                                                .listener(new ItemCountChunkListener())
                                                                .build();
    }

    @Bean
    public ItemReader<SourceTransactionVO> transactionReader(DataSource dataSource) throws Exception {
        return new JdbcPagingItemReaderBuilder<SourceTransactionVO>()
                                                                     .name("Transactions Reader")
                                                                     .dataSource(dataSource)
                                                                     .selectClause("SELECT * ")
                                                                     .fromClause("FROM source_transactions ")
                                                                     .whereClause("WHERE ID <= 1000000 ")
                                                                     .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                                                                     .pageSize(1000)
                                                                     .rowMapper(new SourceTransactionVORowMapper())
                                                                     .build();
    }

    @Bean
    public ItemProcessor<SourceTransactionVO, SourceTransactionVO> transactionProcessor() {
        return (transaction) -> {
            // Thread.sleep(1);
            return transaction;
        };
    }

    @Bean
    public JdbcBatchItemWriter<SourceTransactionVO> transactionWriter(DataSource dataSource) {
        JdbcBatchItemWriter<SourceTransactionVO> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO destination_transactions (id, transaction_date, account_id, amount, created_at) VALUES (:id, :transactionDate, :accountId, :amount, :createdAt)");
        writer.setItemSqlParameterSourceProvider(BeanPropertySqlParameterSource::new);
        writer.afterPropertiesSet();
        return writer;
    }
}
