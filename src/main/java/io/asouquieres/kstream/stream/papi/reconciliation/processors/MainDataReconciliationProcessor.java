package io.asouquieres.kstream.stream.papi.reconciliation.processors;


import io.asouquieres.data.FullData;
import io.asouquieres.data.MainData;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static io.asouquieres.kstream.stream.papi.reconciliation.PapiReconciliationConstants.RECONCILIATION_STORE;


/**
 * This class is an low level .map method equivalent
 * It allows three mains advances usages :
 * - Accessing record headers
 * - Accessing Statestores
 * -
 */
public class MainDataReconciliationProcessor implements Processor<String, MainData, String, FullData> {

    private KeyValueStore<String,FullData> store;
    private ProcessorContext<String, FullData> context;

    @Override
    public void init(ProcessorContext<String, FullData> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;
        store = context.getStateStore(RECONCILIATION_STORE);


        context.schedule(Duration.ofHours(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
            /**
             * Here you have access to the statetore every 1 hour ( in this example )
             * You can :
             *  - Purge the statestore (older than X)
             *  - Forward data based on whatever business rule (you usually prefer to do this based on an input business event !)
             */
        });
    }

    @Override
    public void process(Record<String, MainData> record) {
        var storedValue = store.get(record.key());

        if (storedValue == null) { // FInd or create the existing data
            storedValue = FullData.newBuilder()
                    .setDataId(record.key())
                    .build();
        }

        // Update the data & save
        storedValue.setMainInfo1(record.value().getMainInfo1());
        storedValue.setMainInfo2(record.value().getMainInfo2());
        store.put(record.key(), storedValue);


        // Emit the data : all the time since in this context MainData is the "header data"
        var output = new Record<String,FullData>(record.key(), storedValue,record.timestamp(), record.headers());
        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }

    @Override
    public void close() {

    }
}
