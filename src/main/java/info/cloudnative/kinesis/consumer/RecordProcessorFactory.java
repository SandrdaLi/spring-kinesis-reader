package info.cloudnative.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class RecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor();
    }
}
