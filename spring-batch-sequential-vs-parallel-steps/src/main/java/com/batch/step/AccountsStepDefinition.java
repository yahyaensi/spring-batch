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

import com.batch.model.SourceAccountVO;
import com.batch.model.SourceAccountVORowMapper;

@Configuration
public class AccountsStepDefinition {

    @Bean(name = "accountStep")
    public Step accountStep(JobRepository jobRepository,
                            PlatformTransactionManager transactionManager,
                            ItemReader<SourceAccountVO> accountReader,
                            ItemProcessor<SourceAccountVO, SourceAccountVO> accountProcessor,
                            JdbcBatchItemWriter<SourceAccountVO> accountWriter)
            throws Exception {
        return new StepBuilder("accountStep", jobRepository)
                                                            .<SourceAccountVO, SourceAccountVO>chunk(1000, transactionManager)
                                                            .reader(accountReader)
                                                            .processor(accountProcessor)
                                                            .writer(accountWriter)
                                                            .listener(new ItemCountChunkListener())
                                                            .build();
    }

    @Bean
    public ItemReader<SourceAccountVO> accountReader(DataSource dataSource) throws Exception {
        return new JdbcPagingItemReaderBuilder<SourceAccountVO>()
                                                                 .name("Accounts Reader")
                                                                 .dataSource(dataSource)
                                                                 .selectClause("SELECT * ")
                                                                 .fromClause("FROM source_accounts ")
                                                                 .whereClause("WHERE ID <= 1000000 ")
                                                                 .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                                                                 .pageSize(1000)
                                                                 .rowMapper(new SourceAccountVORowMapper())
                                                                 .build();
    }

    @Bean
    public ItemProcessor<SourceAccountVO, SourceAccountVO> accountProcessor() {
        return (account) -> {
            // Thread.sleep(1);
            return account;
        };
    }

    @Bean
    public JdbcBatchItemWriter<SourceAccountVO> accountWriter(DataSource dataSource) {
        JdbcBatchItemWriter<SourceAccountVO> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO destination_accounts (id, account_number, created_at) VALUES (:id, :accountNumber, :createdAt)");
        writer.setItemSqlParameterSourceProvider(BeanPropertySqlParameterSource::new);
        writer.afterPropertiesSet();
        return writer;
    }
}
