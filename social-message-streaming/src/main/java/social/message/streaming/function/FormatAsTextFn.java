package social.message.streaming.function;

import org.apache.beam.sdk.transforms.SimpleFunction;
import social.message.streaming.dto.SentimentMessage;

public class FormatAsTextFn extends SimpleFunction<SentimentMessage, String> {
  @Override
  public String apply(SentimentMessage input) {
    return input.getContent() + ": " + input.getLang();
  }
}