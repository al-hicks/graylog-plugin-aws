package org.graylog.aws.inputs.codecs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import org.graylog.aws.inputs.kinesisstream.GraylogKeyFields;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KinesisStringCodec extends AbstractCodec {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStringCodec.class);

    public static final String UNKNOWN_SOURCE = "unknown";
    public static final String NAME = "AWSKinesisString";

    private final ObjectMapper objectMapper;

    @Inject
    protected KinesisStringCodec(@Assisted Configuration configuration, ObjectMapper objectMapper) {
        super(configuration);
        this.objectMapper = objectMapper;
    }

    @Nullable
    @Override
    public Message decode(@Nonnull RawMessage rawMessage) {
        try {
            final String input = new String(rawMessage.getPayload(), StandardCharsets.UTF_8);
            GraylogKeyFields keyFields = null;
            try {
                keyFields = objectMapper.readValue(input, GraylogKeyFields.class);
            } catch (JsonProcessingException e) {
                LOG.warn("Error parsing input into GraylogKeyFields:", e);
            }
            return buildMessage(input, keyFields);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't deserialize log data", e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @FactoryClass
    public interface Factory extends Codec.Factory<KinesisStringCodec> {
        @Override
        KinesisStringCodec create(Configuration configuration);

        @Override
        KinesisStringCodec.Config getConfig();
    }

    @ConfigClass
    public static class Config extends AbstractCodec.Config {
    }

    public static Message buildMessage(String raw, GraylogKeyFields keyFields) {
        LOG.info("Raw Input<{}>", raw);
        final Message result = new Message(
            raw,
            (keyFields == null ? UNKNOWN_SOURCE : keyFields.source),
            (keyFields == null ? new DateTime() : new DateTime(keyFields.timestamp))
        );
        LOG.debug("Result<{}>", result);
        return result;
    }
}
