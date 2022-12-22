package io.asouquieres.kstream.helpers;

import java.util.Properties;

/**
 * Hold the static context for the whole stream
 */
public class StreamContext {
    private static String dlqTopic;
    private static Properties props;

    public static Properties getProps() {
        return props;
    }

    public static void setProps(Properties props) {
        StreamContext.props = props;
    }
}
