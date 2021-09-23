package social.message.streaming.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public final class SerializerUtil {
    private SerializerUtil(){
    }

    private static final Logger log = LoggerFactory.getLogger(SerializerUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String serialize(final Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Failed to process object", e);
        }
        return StringUtils.EMPTY;
    }

    public static <T> T deserialize(final String fromString, Class<T> clazz) {
        try {
            return objectMapper.readValue(Optional.ofNullable(fromString).orElse(NullNode.instance.asText()), clazz);
        } catch (IOException e) {
            log.error("Failed to deserialize object", e);
        }
        return null;
    }

}
