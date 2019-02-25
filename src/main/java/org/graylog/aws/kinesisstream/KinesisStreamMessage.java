package org.graylog.aws.kinesisstream;

import org.graylog.aws.cloudwatch.CloudWatchLogEntry;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class KinesisStreamMessage {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamMessage.class);

    private final DateTime timestamp;
    private final String message;

    public KinesisStreamMessage(String message) {
        this.timestamp = new DateTime();
        this.message = message;
    }

    @Nullable
    public static KinesisStreamMessage fromLogEvent(final CloudWatchLogEntry logEvent) {
        if (logEvent.message == null) {
            LOG.warn("Received empty message from stream. Message was: [{}]", logEvent.message);
            return null;
        }

        return new KinesisStreamMessage(logEvent.message);
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

}
