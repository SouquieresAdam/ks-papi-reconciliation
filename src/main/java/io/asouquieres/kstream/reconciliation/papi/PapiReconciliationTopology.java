package io.asouquieres.kstream.reconciliation.papi;

import io.asouquieres.data.*;

import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.reconciliation.papi.processors.MainDataReconciliationProcessor;
import io.asouquieres.kstream.reconciliation.papi.processors.SatelliteDataAProcessor;
import io.asouquieres.kstream.reconciliation.papi.processors.SatelliteDataBProcessor;
import io.asouquieres.kstream.reconciliation.papi.processors.SatelliteDataCProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

import static io.asouquieres.kstream.reconciliation.ReconciliationConstants.*;

public class PapiReconciliationTopology {

    public static Topology getTopology() {

        // Create the internal reconciliation statestore
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(RECONCILIATION_STORE), Serdes.String(), AvroSerdes.<FullData>get()));

        // Stream & re-key each input flow to ensure they have the same partitioning
        var mainDataStream = builder.stream(MAIN_DATA_TOPIC, Consumed.with(Serdes.String(), AvroSerdes.<MainData>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<MainData>get()).withName(MAIN_DATA_BY_CORRELATION_KEY));

        var satelliteDataAStream = builder.stream(SATELLITE_INFO_A, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataA>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataA>get()).withName(SATELLITEDATAA_BY_CORRELATION_KEY));
        var satelliteDataBStream = builder.stream(SATELLITE_INFO_B, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataB>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataB>get()).withName(SATELLITEDATAB_BY_CORRELATION_KEY));
        var satelliteDataCStream = builder.stream(SATELLITE_INFO_C, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataC>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataC>get()).withName(SATELLITEDATAC_BY_CORRELATION_KEY));

        // Integrate each of the flow with their own Processor class containing "Business Rules"
        mainDataStream.process(MainDataReconciliationProcessor::new, RECONCILIATION_STORE) // Do not forget to assign the statestore to the operation

                // All output must be merged to be published in the same final topic
                .merge(satelliteDataAStream.process(SatelliteDataAProcessor::new, RECONCILIATION_STORE))
                .merge(satelliteDataBStream.process(SatelliteDataBProcessor::new, RECONCILIATION_STORE))
                .merge(satelliteDataCStream.process(SatelliteDataCProcessor::new, RECONCILIATION_STORE))
                .to(FULL_DATA_OUTPUT, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}
