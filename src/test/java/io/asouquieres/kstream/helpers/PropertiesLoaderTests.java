package io.asouquieres.kstream.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class PropertiesLoaderTests {

    private Properties loadedProperties;

    @BeforeEach
    public void init() {
        loadedProperties = PropertiesLoader.fromYaml("application.yml");
    }

    @Test
    public void shouldLoadStringProperties() {
        assertEquals("test", loadedProperties.get("string"));
    }

    @Test
    public void shouldLoadBooleanProperties() {
        assertEquals(true, loadedProperties.get("boolean"));
    }

    @Test
    public void shouldLoadIntegerProperties() {
        assertEquals(5, loadedProperties.get("int"));
    }

    @Test
    public void shouldLoadDeepProperties() {
        assertEquals(42, loadedProperties.get("parent.child.subchild"));
    }
}
