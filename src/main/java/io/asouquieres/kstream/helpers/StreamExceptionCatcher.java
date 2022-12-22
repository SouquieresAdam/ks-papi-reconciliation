package io.asouquieres.kstream.helpers;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

public class StreamExceptionCatcher {

    public static String DLQ_NAME = "dlq.topic";
    /**
     *
     */
    public static <SK, SV, FV> KStream<SK, SV> check(KStream<SK, MayBeException<SV,FV>> stream) {

        var branches =
                stream.split()
                        .branch((key, value) -> value.streamException != null, Branched.as("exception"))
                        .defaultBranch(Branched.as("success"));

        branches.get("exception")
                .map( (k,v) -> KeyValue.pair(k.toString(), v.streamException.toString()))
                .to(StreamContext.getProps().getProperty(DLQ_NAME), Produced.with(Serdes.String(), Serdes.String()));

        return branches.get("success")
                .mapValues( v -> v.streamValue);
    }
}
