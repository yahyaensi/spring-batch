package com.batch.chunk.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;

import com.batch.model.Line;
import com.batch.utils.FileUtils;

public class LineReader implements ItemReader<Line>, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LineReader.class);
    private FileUtils fu;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        fu = new FileUtils("taskletsvschunks/input/tasklets-vs-chunks.csv");
        logger.info("Line Reader initialized.");
    }

    @Override
    public Line read() {
        Line line = fu.readLine();
        if (line != null) {
            logger.info("Read line: " + line);
        }
        return line;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        fu.closeReader();
        logger.info("Line Reader ended.");
        return ExitStatus.COMPLETED;
    }
}
