package org.graylog.aws.inputs.kinesisstream;

import org.graylog.aws.cloudwatch.CloudWatchLogEntry;
import org.graylog.aws.kinesisstream.KinesisStreamMessage;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class KinesisStreamMessageTest {

    @Test
    public void testFromPartsDoesNotFailWithMissingIntegerFields() throws Exception {
        final String message = "{ \"app\": \"uav\", \"msg\": \"Hello World!\" }";

        final CloudWatchLogEntry logEvent = new CloudWatchLogEntry("helloStream", "helloGroup", DateTime.now().getMillis() / 1000, message);
        final KinesisStreamMessage m = KinesisStreamMessage.fromLogEvent(logEvent);

        assertEquals(m.getMessage(), message);
    }

}