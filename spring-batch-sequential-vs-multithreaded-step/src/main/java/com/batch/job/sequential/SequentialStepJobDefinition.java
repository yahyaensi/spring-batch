package com.batch.job.sequential;

import org.springframework.batch.core.Job;
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
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.model.TransactionVO;
import com.batch.step.ItemCountChunkListener;

@Configuration
public class SequentialStepJobDefinition {

    @Bean(name = "sequentialStep")
    public Step sequentialStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager,
                               ItemReader<TransactionVO> itemReader,
                               ItemProcessor<TransactionVO, TransactionVO> itemProcessor,
                               FlatFileItemWriter<TransactionVO> itemWriter)
            throws Exception {

        return new StepBuilder("sequentialStep", jobRepository)
                                                               .<TransactionVO, TransactionVO>chunk(1000, transactionManager)
                                                               .reader(itemReader)
                                                               .processor(itemProcessor)
                                                               .writer(itemWriter)
                                                               .listener(new ItemCountChunkListener())
                                                               .build();
    }

    @Bean
    public Job sequentialStepJob(JobRepository jobRepository, @Qualifier("sequentialStep") Step sequentialStep) {

        return new JobBuilder("sequentialStepJob", jobRepository)
                                                                 .incrementer(new RunIdIncrementer())
                                                                 .flow(sequentialStep)
                                                                 .end()
                                                                 .build();
    }
}
