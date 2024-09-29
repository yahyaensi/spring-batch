package com.batch.job.asyncprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

public class ItemCountChunkListener implements ChunkListener {

    private static final Logger logger = LoggerFactory.getLogger(ItemCountChunkListener.class);

    @Override
    public void beforeChunk(ChunkContext context) {
        // You can use this method for any setup needed before a chunk begins processing.
        // logger.info("Before processing chunk: {}", context);
    }

    @Override
    public void afterChunk(ChunkContext context) {
        // Calculate the number of items processed in the chunk.
        long itemsRead = context.getStepContext().getStepExecution().getReadCount();

        // Print the number of items processed.
        logger.info("Number of items read: {}", itemsRead);
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        // You can use this method to handle errors that occur during chunk processing.
        logger.error("Error occurred during chunk processing: {}", context);
    }
}
