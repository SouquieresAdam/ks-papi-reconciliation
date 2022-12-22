package io.asouquieres.kstream.stream.papi.reconciliation.processors;


import io.asouquieres.data.FullData;
import io.asouquieres.data.MainData;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class MainDataReconciliationProcessor implements Processor<String, MainData, String, FullData> {

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, FullData> context) {
        Processor.super.init(context);
    }

    @Override
    public void process(Record<String, MainData> record) {

    }

    @Override
    public void close() {

    }
}
