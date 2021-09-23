package social.message.streaming.function;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import social.message.streaming.window.WindowTiming;
import social.message.streaming.entity.SentimentRow;
import social.message.streaming.entity.SentimentLabelCounter;

@AllArgsConstructor(staticName = "of")
public class BuildTopicRowFn
    extends DoFn<KV<Integer, SentimentLabelCounter>, KV<Integer, SentimentRow>> {
  private static final long serialVersionUID = -675648787095552004L;

  @SuppressWarnings("unused")
  @ProcessElement
  public void processElement(ProcessContext c, IntervalWindow window) {
    final SentimentLabelCounter labelCounter = c.element().getValue();
    final SentimentRow topicRow =
        new SentimentRow(
            c.element().getKey(),
            labelCounter,
            WindowTiming.of(window.start(), window.end()));
    c.output(KV.of(c.element().getKey(), topicRow));
  }
}
