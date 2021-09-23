package social.message.streaming.tranform;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import social.message.streaming.entity.SentimentRow;
import social.message.streaming.function.BuildTopicRowFn;
import social.message.streaming.function.LabelsCombineFn;
import social.message.streaming.type.SentimentLabel;

import java.io.Serializable;

@AllArgsConstructor(staticName = "of")
public class AggregateSentimentLabels extends PTransform<PCollection<KV<Integer, SentimentLabel>>, PCollection<KV<Integer, SentimentRow>>>
    implements Serializable {
  private static final long serialVersionUID = -2001070150030021354L;

  @Override
  protected String getKindString() {
    return "CountSentimentLabels";
  }

  @Override
  public PCollection<KV<Integer, SentimentRow>> expand(final PCollection<KV<Integer, SentimentLabel>> input) {
    input.setWindowingStrategyInternal(
        input
            .getWindowingStrategy()
            .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES));

    final PCollection<KV<Integer, SentimentRow>> combined =
        input.apply(Combine.perKey(new LabelsCombineFn()))
            .apply(ParDo.of(BuildTopicRowFn.of()));

    return combined;
  }
}
