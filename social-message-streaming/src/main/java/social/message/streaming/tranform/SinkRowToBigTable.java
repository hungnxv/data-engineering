package social.message.streaming.tranform;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import social.message.streaming.entity.SentimentRow;
import social.message.streaming.function.MapToMutations;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true, fluent = true)
public class SinkRowToBigTable extends PTransform<PCollection<KV<Integer, SentimentRow>>, PDone> {


    private static final long serialVersionUID = -6547452713948876084L;
    private static final String PROJECT_ID = "google cloud setting";
    private static final String INSTANCE_ID = "google instance id";
    private static final String TABLE_ID = "test_message_v1";

    @Override
    public PDone expand(final PCollection<KV<Integer, SentimentRow>> input) {
        return input
            .apply("Row-To-Mutation", ParDo.of(new MapToMutations()))
            .apply(
                BigtableIO.write()
                    .withProjectId(PROJECT_ID)
                    .withInstanceId(INSTANCE_ID)
                    .withTableId(TABLE_ID));
    }
}
