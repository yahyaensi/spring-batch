package com.batch.step;

import java.util.Collections;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.batch.model.TransactionVO;
import com.batch.model.TransactionVORowMapper;

@Configuration
public class StepDefinition {

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
}
