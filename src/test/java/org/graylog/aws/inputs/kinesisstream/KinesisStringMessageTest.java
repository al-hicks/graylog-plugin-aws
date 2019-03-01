package org.graylog.aws.inputs.kinesisstream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.graylog.aws.inputs.codecs.KinesisStringCodec;
import org.graylog2.plugin.Message;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class KinesisStringMessageTest {

    private final String LOGFORMAT = "{ \"timestamp\": \"%s\" , \"level\": \"%s\", \"message\": \"%s\", \"source\": \"%s\" }";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testRandomStringInput() throws Exception {
        final String input = "String to log-" + UUID.randomUUID().toString();

        GraylogKeyFields keyFields = null;
        try {
            keyFields = objectMapper.readValue(input, GraylogKeyFields.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Message result = KinesisStringCodec.buildMessage(input, keyFields);

        assertEquals(input, result.getMessage());
    }

    @Test
    public void testValidGraylogKeyFieldsInput() throws Exception {
        final String source = "demo";
        final String message = "String-"+UUID.randomUUID().toString();
        final long timestamp = new DateTime().getMillis();

        final String input = String.format(LOGFORMAT,
                timestamp,
                "info",
                message,
                source);

        GraylogKeyFields keyFields = null;
        try {
            keyFields = objectMapper.readValue(input, GraylogKeyFields.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Message result = KinesisStringCodec.buildMessage(input, keyFields);

        assertEquals(input, result.getMessage());
        assertEquals(source, result.getSource());
        assertEquals(timestamp, result.getTimestamp().getMillis());
    }

    @Test
    public void testInvalidGraylogKeyFieldsInput() throws Exception {
        final String source = "demo";
        final String message = "String-"+UUID.randomUUID().toString();
        final String timestamp = "xxxx";

        final String input = String.format(LOGFORMAT,
                timestamp,
                "info",
                message,
                source);

        GraylogKeyFields keyFields = null;
        try {
            keyFields = objectMapper.readValue(input, GraylogKeyFields.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Message result = KinesisStringCodec.buildMessage(input, keyFields);

        assertEquals(input, result.getMessage());
        assertEquals(KinesisStringCodec.UNKNOWN_SOURCE, result.getSource());
        assertTrue(new Interval(
                        new DateTime().minusMinutes(3),
                        new DateTime().plusMinutes(3)
                    ).contains(result.getTimestamp()));
    }

}