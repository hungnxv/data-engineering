package social.message.streaming.function;


import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import social.message.streaming.entity.SentimentRow;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MapToMutations extends DoFn<KV<Integer, SentimentRow>, KV<ByteString, Iterable<Mutation>>> {
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormat.forPattern("yyyyMMddHHmmssSSS").withZoneUTC();
        private static final String ERROR_MESSAGE = "Error while putting to BigTable";

        private transient DataflowPipelineOptions dataflowOptions;

        @SuppressWarnings("unused")
        @StartBundle
        public void startBundle(final StartBundleContext context) {
            if (dataflowOptions == null) {
                dataflowOptions = context.getPipelineOptions().as(DataflowPipelineOptions.class);
            }
        }

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(final ProcessContext context, final BoundedWindow window) {
            final KV<Integer, SentimentRow> input = context.element();
            final Instant timestamp = context.timestamp();
            String key = input.getKey().toString() + "#" + DATE_TIME_FORMATTER.print(timestamp);
            try {
                context.output(
                    KV.of(ByteString.copyFromUtf8(key), input.getValue().toMutations(context.timestamp())));
            } catch (final Exception exception) {
                log.error("{}", ERROR_MESSAGE + input, exception);
            }
        }
    }
