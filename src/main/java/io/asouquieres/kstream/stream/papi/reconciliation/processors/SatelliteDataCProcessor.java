package io.asouquieres.kstream.stream.papi.reconciliation.processors;


import io.asouquieres.data.FullData;
import io.asouquieres.data.SatelliteDataC;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.codehaus.plexus.util.StringUtils;

import static io.asouquieres.kstream.stream.papi.reconciliation.PapiReconciliationConstants.RECONCILIATION_STORE;

public class SatelliteDataCProcessor implements Processor<String, SatelliteDataC, String, FullData> {

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
    public void process(Record<String, SatelliteDataC> record) {
        var storedValue = store.get(record.key());

        if (storedValue == null) { // FInd or create the existing data
            storedValue = FullData.newBuilder()
                    .setDataId(record.key())
                    .build();
        }

        // Update the data & save
        storedValue.setSatelliteInfo3(record.value().getSatelliteInfoC());
        store.put(record.key(), storedValue);


        // Do not Emit the data if we miss some main information
        if(StringUtils.isEmpty(storedValue.getMainInfo1())  || StringUtils.isEmpty(storedValue.getMainInfo2())) {
            return;
        }

        var output = new Record<>(record.key(), storedValue,record.timestamp(), record.headers());
        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }

    @Override
    public void close() {

    }
}
