package com.batch.tasklet.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.batch.model.Line;
import com.batch.utils.FileUtils;

public class LinesReader implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesReader.class);

    private List<Line> lines;

    private FileUtils fu;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        lines = new ArrayList<>();
        fu = new FileUtils("taskletsvschunks/input/tasklets-vs-chunks.csv");
        logger.info("Lines Reader initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Line line = fu.readLine();
        while (line != null) {
            lines.add(line);
            logger.info("Read line: " + line.toString());
            line = fu.readLine();
        }
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        fu.closeReader();
        stepExecution.getJobExecution()
                     .getExecutionContext()
                     .put("lines", this.lines);
        logger.info("Lines Reader ended.");
        return ExitStatus.COMPLETED;
    }
}
