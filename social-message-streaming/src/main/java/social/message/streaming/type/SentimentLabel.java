package social.message.streaming.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

public enum SentimentLabel implements Serializable {
    NEGATIVE("negative"),
    NEUTRAL("neutral"),
    POSITIVE("positive");

    private String label;

    SentimentLabel(String label){
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    private static final Map<String, SentimentLabel> VALUES = Arrays.stream(values()).collect(collectingAndThen(
        toMap(Enum::name, identity()), Collections::unmodifiableMap));

    public static SentimentLabel of(String type) {
        if(type == null)
            return null;
        return VALUES.get(type.toLowerCase());
    }

    @JsonValue
    public String getValue() {
        return name().toLowerCase();
    }
}
