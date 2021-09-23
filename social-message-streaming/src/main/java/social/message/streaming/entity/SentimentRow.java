package social.message.streaming.entity;

import com.google.bigtable.v2.Mutation;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import social.message.streaming.window.WindowTiming;

import java.util.ArrayList;
import java.util.List;

import static social.message.streaming.entity.SentimentLabelCounter.SENTIMENT_FAMILY;

@Getter
@Setter
public class SentimentRow implements BigTableMutableRow {
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
    public static final String CREATED_AT_COL = "createdAt";

    private Integer companyId;
    private SentimentLabelCounter labelCounter;
    private WindowTiming windowTiming;

    @Builder
    public SentimentRow(final Integer companyId, final SentimentLabelCounter labelCounter, final WindowTiming windowTiming) {
        this.companyId = companyId;
        this.labelCounter = labelCounter;
        this.windowTiming = windowTiming;
    }

    public List<Mutation> toMutations(final Instant timestamp) {
        final List<Mutation> mutations = new ArrayList<>();
        if (labelCounter != null) {
            mutations.addAll(labelCounter.toMutations(timestamp));
        }
        mutations.add(
            buildSetMutation(SENTIMENT_FAMILY, CREATED_AT_COL, DATE_TIME_FORMATTER.print(timestamp)));
        return mutations;
    }

    public SentimentRow duplicate() {
        return new SentimentRow(companyId, labelCounter.duplicate(), windowTiming);
    }

    /*private Mutation buildSetMutation(
        final String columnFamily, final String columnQualifier, final Object value) {
        return buildSetMutation(
            columnFamily, columnQualifier, ByteString.copyFromUtf8(String.valueOf(value)));
    }*/
}
