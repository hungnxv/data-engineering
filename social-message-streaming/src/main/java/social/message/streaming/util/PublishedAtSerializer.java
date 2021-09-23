package social.message.streaming.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PublishedAtSerializer extends StdSerializer<LocalDateTime> {
    private static DateTimeFormatter FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final long serialVersionUID = 3562612066442501335L;

    public PublishedAtSerializer() {
        super(LocalDateTime.class);
    }

    @Override
    public void serialize(
        final LocalDateTime value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
        gen.writeString(FORMAT.format(value));
    }
}