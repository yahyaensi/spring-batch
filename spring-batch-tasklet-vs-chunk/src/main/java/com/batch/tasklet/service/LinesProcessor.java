package com.batch.tasklet.service;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;

import com.batch.model.Line;

public class LinesProcessor implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesProcessor.class);

    private List<Line> lines;

    @SuppressWarnings("unchecked")
    @Override
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution()
                                                         .getExecutionContext();
        this.lines = (List<Line>) executionContext.get("lines");
        logger.info("Lines Processor initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        // uncomment this exception to test conditional flow
        // boolean a = true;
        // if (a) {
        // throw new RuntimeException("Exception occured");
        // }
        for (Line line : lines) {
            long age = ChronoUnit.YEARS.between(line.getDob(), LocalDate.now());
            logger.info("Calculated age " + age + " for line " + line.toString());
            line.setAge(age);
        }
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Lines Processor ended.");
        return ExitStatus.COMPLETED;
    }
}
