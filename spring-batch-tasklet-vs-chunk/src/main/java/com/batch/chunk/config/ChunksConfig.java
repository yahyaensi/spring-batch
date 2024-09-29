package com.batch.chunk.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.chunk.service.LineProcessor;
import com.batch.chunk.service.LineReader;
import com.batch.chunk.service.LinesWriter;
import com.batch.model.Line;

@Configuration
public class ChunksConfig {

    @Bean
    public ItemReader<Line> itemReader() {
        return new LineReader();
    }

    @Bean
    public ItemProcessor<Line, Line> itemProcessor() {
        return new LineProcessor();
    }

    @Bean
    public ItemWriter<Line> itemWriter() {
        return new LinesWriter();
    }

    @Bean(name = "chunksStep")
    protected Step chunksStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                              ItemReader<Line> reader, ItemProcessor<Line, Line> processor, ItemWriter<Line> writer) {
        return new StepBuilder("processLines", jobRepository).<Line, Line>chunk(2, transactionManager)
                                                             .reader(reader)
                                                             .processor(processor)
                                                             .writer(writer)
                                                             .build();
    }

    @Bean(name = "chunksJob")
    public Job job(JobRepository jobRepository, @Qualifier("chunksStep") Step chunksStep) {
        return new JobBuilder("chunksJob", jobRepository)
                                                         .start(chunksStep)
                                                         .build();
    }

}
