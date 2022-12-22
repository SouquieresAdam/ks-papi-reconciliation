package io.asouquieres.kstream.stream.simpleavro;

import io.asouquieres.kstream.helpers.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import io.asouquieres.data.MainData;

public class SimpleStreamWithAvroTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        var sourceStream = builder.stream(SimpleStreamWithAvroConstants.SOURCE_TOPIC, Consumed.with(Serdes.String(), AvroSerdes.<MainData>get()));
        sourceStream
                .map(KeyValue::pair)
                .to(SimpleStreamWithAvroConstants.FILTERED_TOPIC, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}
