package io.asouquieres.kstream.helpers;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.plexus.util.StringUtils;

import java.util.HashMap;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;

public class AvroSerdes {


    /**
     * Will return automatically the correct serdes for any Avro Object
     * @return a serdes for any Avro Object
     * @param <T> Avro model type
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> get() {
        var specificSerdes = new SpecificAvroSerde<T>();

        var propMap = new HashMap<String, Object>();
        propMap.put(SCHEMA_REGISTRY_URL_CONFIG, StreamContext.getProps().get(SCHEMA_REGISTRY_URL_CONFIG));
        if(!StringUtils.isEmpty((String)StreamContext.getProps().get(BASIC_AUTH_CREDENTIALS_SOURCE)) && !StringUtils.isEmpty((String)StreamContext.getProps().get(USER_INFO_CONFIG))) {
            propMap.put(BASIC_AUTH_CREDENTIALS_SOURCE, StreamContext.getProps().get(BASIC_AUTH_CREDENTIALS_SOURCE));
            propMap.put(USER_INFO_CONFIG, StreamContext.getProps().get(USER_INFO_CONFIG));
        }

        specificSerdes.configure(propMap, false);
        return specificSerdes;
    }
}
