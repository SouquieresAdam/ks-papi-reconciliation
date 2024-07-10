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
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import static io.asouquieres.kstream.reconciliation.ReconciliationConstants.*;

public class PapiReconciliationTopology {

    public static Topology getTopology() {

        // Create the internal reconciliation statestore
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(Statestores.RECONCILIATION_STORE), Serdes.String(), AvroSerdes.<FullData>get()));



        // Stream & re-key each input flow to ensure they have the same partitioning
        var mainDataStream = builder.stream(Topics.MAIN_DATA_TOPIC, Consumed.with(Serdes.String(), AvroSerdes.<MainData>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<MainData>get()).withName(Repartitions.MAIN_DATA_BY_CORRELATION_KEY));

        var satelliteDataAStream = builder.stream(Topics.SATELLITE_INFO_A, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataA>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataA>get()).withName(Repartitions.SATELLITEDATAA_BY_CORRELATION_KEY));
        var satelliteDataBStream = builder.stream(Topics.SATELLITE_INFO_B, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataB>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataB>get()).withName(Repartitions.SATELLITEDATAB_BY_CORRELATION_KEY));
        var satelliteDataCStream = builder.stream(Topics.SATELLITE_INFO_C, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataC>get()))
                .selectKey((k,v) -> v.getDataId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<SatelliteDataC>get()).withName(Repartitions.SATELLITEDATAC_BY_CORRELATION_KEY));

        // Integrate each of the flow with their own Processor class containing "Business Rules"
        mainDataStream.process(MainDataReconciliationProcessor::new, Statestores.RECONCILIATION_STORE) // Do not forget to assign the statestore to the operation

                // All output must be merged to be published in the same final topic
                .merge(satelliteDataAStream.process(SatelliteDataAProcessor::new, Statestores.RECONCILIATION_STORE))
                .merge(satelliteDataBStream.process(SatelliteDataBProcessor::new, Statestores.RECONCILIATION_STORE))
                .merge(satelliteDataCStream.process(SatelliteDataCProcessor::new, Statestores.RECONCILIATION_STORE))
                .to(Topics.FULL_DATA_OUTPUT, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}
