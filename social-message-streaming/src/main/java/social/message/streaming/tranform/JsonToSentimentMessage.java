package social.message.streaming.tranform;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.util.SerializerUtil;

@AllArgsConstructor(staticName = "of")
@Slf4j
public class JsonToSentimentMessage
    extends PTransform<PCollection<String>, PCollection<SentimentMessage>> {

  @Override
  public PCollection<SentimentMessage> expand(final PCollection<String> input) {
    return input
        .apply("JsonParser", ParDo.of(new ParseFn()))
        .setCoder(SerializableCoder.of(SentimentMessage.class));
  }

  private static class ParseFn extends DoFn<String, SentimentMessage> {

    @ProcessElement
    public void processElement(final ProcessContext context) {
      final String json = context.element();
      try {
        SentimentMessage sentimentMessage = SerializerUtil.deserialize(json, SentimentMessage.class);

        if(sentimentMessage != null){
          context.output(sentimentMessage);
        }
      } catch (final IllegalArgumentException ex) {
        log.error("Error parsing {}: {}", ex, json);
      }
    }
  }
}
