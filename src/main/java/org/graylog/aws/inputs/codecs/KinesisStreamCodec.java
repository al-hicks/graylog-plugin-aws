package org.graylog.aws.inputs.codecs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import org.graylog.aws.AWS;
import org.graylog.aws.AWSObjectMapper;
import org.graylog.aws.cloudwatch.CloudWatchLogEntry;
import org.graylog.aws.kinesisstream.KinesisStreamMessage;
import org.graylog.aws.inputs.cloudtrail.CloudTrailCodec;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.Codec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class KinesisStreamCodec extends CloudWatchLogDataCodec {
    public static final String NAME = "AWSKinesisStream";

    @Inject
    public KinesisStreamCodec(@Assisted Configuration configuration, @AWSObjectMapper ObjectMapper objectMapper) {
        super(configuration, objectMapper);
    }

    @Nullable
    @Override
    public Message decodeLogData(@Nonnull final CloudWatchLogEntry logEvent, @Nonnull final String logGroup, @Nonnull final String logStream) {
        try {
            final KinesisStreamMessage rawKinesesMessage = KinesisStreamMessage.fromLogEvent(logEvent);

            if (rawKinesesMessage == null) {
                return null;
            }

            final String source = configuration.getString(CloudTrailCodec.Config.CK_OVERRIDE_SOURCE, "aws-kinesis-stream");
            final Message result = new Message(
                    buildSummary(rawKinesesMessage),
                    source,
                    rawKinesesMessage.getTimestamp()
            );
            result.addFields(buildFields(rawKinesesMessage));
            result.addField(AWS.FIELD_LOG_GROUP, logGroup);
            result.addField(AWS.FIELD_LOG_STREAM, logStream);
            result.addField(AWS.SOURCE_GROUP_IDENTIFIER, true);

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize AWS FlowLog record.", e);
        }
    }

    private String buildSummary(KinesisStreamMessage msg) {
        return new StringBuilder()
                .append(msg.getMessage())
                .toString();
    }

    private Map<String, Object> buildFields(KinesisStreamMessage msg) {
        return new HashMap<String, Object>() {{
            put("raw_message", msg.getMessage());
        }};
    }

    @Override
    public String getName() {
        return NAME;
    }

    @FactoryClass
    public interface Factory extends Codec.Factory<KinesisStreamCodec> {
        @Override
        KinesisStreamCodec create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends AbstractCodec.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            return new ConfigurationRequest();
        }

        @Override
        public void overrideDefaultValues(@Nonnull ConfigurationRequest cr) {
        }
    }
}
