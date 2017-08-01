package info.cloudnative.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.time.*;


/**
 * @since 1.0
 */
public class RecordProcessor implements IRecordProcessor {

	protected Logger log = LoggerFactory.getLogger(RecordProcessor.class);

	private String shardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 1;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	@Override
	public void initialize(InitializationInput initializationInput) {
		this.shardId = initializationInput.getShardId();
		log.info("initialize():  shardId={}", this.shardId);
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {

        Instant instantPostGetRecords = Instant.now();
        LocalDateTime dateTimePostGetRecords;
        Instant instantPostException;
        LocalDateTime dateTimePostException;
        Instant instantFinally;
        LocalDateTime dateTimeFinally;
        IRecordProcessorCheckpointer checkpointer = null;

        try {
            List<Record> records = processRecordsInput.getRecords();
            checkpointer = processRecordsInput.getCheckpointer();

            dateTimePostGetRecords = LocalDateTime.ofInstant(instantPostGetRecords, ZoneId.of("Asia/Tokyo"));

            log.info("******PostGetRecords******");
            log.info("Processing {} records from {} at {}.", records.size(), this.shardId, dateTimePostGetRecords);

            // // test expiration
            // try {
            //     TimeUnit.SECONDS.sleep(350);
            // } catch (InterruptedException e) {
            //     log.info("-- InterruptedException:  {}", e);
            // }

            // Process records and perform all exception handling.
            processRecordsWithRetries(records);
        } catch (Exception e) {
            instantPostException = Instant.now();
            dateTimePostException = LocalDateTime.ofInstant(instantPostException, ZoneId.of("Asia/Tokyo"));

            log.info("******PostException******");
            log.info("Exception happened from {} at {}.", this.shardId, instantPostException);
            log.error(e.getMessage());
        } finally {
            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(checkpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }

            instantFinally = Instant.now();
            dateTimeFinally = LocalDateTime.ofInstant(instantFinally, ZoneId.of("Asia/Tokyo"));

            log.info("******Finally****** at {}", dateTimeFinally);
            log.info("Records process duration: {}", Duration.between(instantPostGetRecords, instantFinally));
        }


/*
        List<Record> records = processRecordsInput.getRecords();
        IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

        log.info("Processing {} records from {} at {}.", records.size(), this.shardId, LocalDateTime.now());

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
*/
	}


    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     * 
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) throws Exception{
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    log.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    log.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                throw new Exception("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     * 
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) throws Exception{
        // TODO Add your own record processing logic here



        // For this app, we interpret the payload as UTF-8 chars.
        String data = decoder.decode(record.getData()).toString();
        // Assume this record came from AmazonKinesisSample and log its age.
        long mynumber = new Long(data);
 
        if ((mynumber % 2) == 0) {
            // test expiration
            try {
                TimeUnit.SECONDS.sleep(mynumber);
            } catch (InterruptedException e) {
                log.info("-- InterruptedException:  {}", e);
            }
            log.info("{} processed from {} but delayed!!!!!!!!!!!!", mynumber, this.shardId);
        } else {
            log.info("{} processed from {}!!!!!!!!!!!!", mynumber, this.shardId);
        }




        // String data = null;
        // try {
        //     // For this app, we interpret the payload as UTF-8 chars.
        //     data = decoder.decode(record.getData()).toString();
        //     // Assume this record came from AmazonKinesisSample and log its age.
        //     long recordCreateTime = new Long(data.substring("testData-".length()));
        //     long ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime;

        //     log.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data + ", Created "
        //             + ageOfRecordInMillis + " milliseconds ago.");
        // } catch (NumberFormatException e) {
        //     log.info("Record does not match sample record format. Ignoring record with data; " + data);
        // } catch (CharacterCodingException e) {
        //     log.error("Malformed data: " + data, e);
        // }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.info("Shutting down record processor for shard: " + this.shardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }


    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        log.info("Checkpointing shard " + this.shardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                log.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.debug("Interrupted sleep", e);
            }
        }
    }
}
