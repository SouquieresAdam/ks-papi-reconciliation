package io.asouquieres.kstream.reconciliation.cogroup;

import io.asouquieres.data.*;
import io.asouquieres.kstream.helpers.AvroSerdes;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import static io.asouquieres.kstream.reconciliation.ReconciliationConstants.*;

public class CogroupReconciliationTopology {

    public static Topology getTopology() {

        // Create the internal reconciliation statestore
        StreamsBuilder builder = new StreamsBuilder();
        // Stream & re-key each input flow to ensure they have the same partitioning
        var mainDataStream = builder.stream(Topics.MAIN_DATA_TOPIC, Consumed.with(Serdes.String(), AvroSerdes.<MainData>get()))
                .groupBy((k,v) -> v.getDataId(), Grouped.with(Repartitions.MAIN_DATA_BY_CORRELATION_KEY,Serdes.String(), AvroSerdes.get()));

        var satelliteDataAStream = builder.stream(Topics.SATELLITE_INFO_A, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataA>get()))
                .groupBy((k,v) -> v.getDataId(), Grouped.with(Repartitions.SATELLITEDATAA_BY_CORRELATION_KEY,Serdes.String(), AvroSerdes.get()));
        var satelliteDataBStream = builder.stream(Topics.SATELLITE_INFO_B, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataB>get()))
                .groupBy((k,v) -> v.getDataId(), Grouped.with(Repartitions.SATELLITEDATAB_BY_CORRELATION_KEY,Serdes.String(), AvroSerdes.get()));
        var satelliteDataCStream = builder.stream(Topics.SATELLITE_INFO_C, Consumed.with(Serdes.String(), AvroSerdes.<SatelliteDataC>get()))
                .groupBy((k,v) -> v.getDataId(), Grouped.with(Repartitions.SATELLITEDATAC_BY_CORRELATION_KEY,Serdes.String(), AvroSerdes.get()));

        // Integrate each of the flow with their own Processor class containing "Business Rules"
       mainDataStream.<FullData>cogroup((k,i,o) -> {
                    o.setDataId(i.getDataId());
                    o.setMainInfo1(i.getMainInfo1());
                    o.setMainInfo2(i.getMainInfo2());
                    return o;
                })
               .cogroup(satelliteDataAStream,(k,i,o) -> {
                   o.setSatelliteInfo1(i.getSatelliteInfoA());
                   return o;
               })
               .cogroup(satelliteDataBStream,(k,i,o) -> {
                   o.setSatelliteInfo2(i.getSatelliteInfoB());
                   return o;
               })
               .cogroup(satelliteDataCStream,(k,i,o) -> {
                   o.setSatelliteInfo3(i.getSatelliteInfoC());
                   return o;
               })
//             .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(2)))
               .aggregate(() -> FullData.newBuilder().build(), Materialized.with(Serdes.String(), AvroSerdes.<FullData>get()))
               .toStream()
               .filter((k,v) -> !StringUtils.isEmpty(v.getMainInfo1()))
               .to(Topics.FULL_DATA_OUTPUT, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}
