package org.graylog.aws.inputs.kinesisstream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GraylogKeyFields {

    @JsonProperty("timestamp")
    public long timestamp;

    @JsonProperty("source")
    public String source;
}
