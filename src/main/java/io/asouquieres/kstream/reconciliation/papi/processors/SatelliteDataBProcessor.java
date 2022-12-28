package io.asouquieres.kstream.reconciliation.papi.processors;


import io.asouquieres.data.FullData;
import io.asouquieres.data.SatelliteDataB;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.codehaus.plexus.util.StringUtils;

import static io.asouquieres.kstream.reconciliation.ReconciliationConstants.RECONCILIATION_STORE;

public class SatelliteDataBProcessor implements Processor<String, SatelliteDataB, String, FullData> {

    private KeyValueStore<String,FullData> store;
    private ProcessorContext<String, FullData> context;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, FullData> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;
        store = context.getStateStore(RECONCILIATION_STORE);

    }

    @Override
    public void process(Record<String, SatelliteDataB> record) {
        var storedValue = store.get(record.key());

        if (storedValue == null) { // FInd or create the existing data
            storedValue = FullData.newBuilder()
                    .setDataId(record.key())
                    .build();
        }

        // Update the data & save
        storedValue.setSatelliteInfo2(record.value().getSatelliteInfoB());
        store.put(record.key(), storedValue);


        // Do not Emit the data if we miss some main information
        if(StringUtils.isEmpty(storedValue.getMainInfo1())  || StringUtils.isEmpty(storedValue.getMainInfo2())) {
            return;
        }

        var output = new Record<>(record.key(), storedValue,record.timestamp(), record.headers());
        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }
    public void close() {

    }
}
