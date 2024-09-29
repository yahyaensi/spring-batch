package com.batch.chunk.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import com.batch.model.Line;
import com.batch.utils.FileUtils;

public class LinesWriter implements ItemWriter<Line>, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesWriter.class);
    private FileUtils fu;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        fu = new FileUtils("output.csv");
        logger.info("Line Writer initialized.");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        fu.closeWriter();
        logger.info("Line Writer ended.");
        return ExitStatus.COMPLETED;
    }

    @Override
    public void write(Chunk<? extends Line> lines) {
        for (Line line : lines) {
            fu.writeLine(line);
            logger.info("Wrote line " + line.toString());
        }
    }
}
