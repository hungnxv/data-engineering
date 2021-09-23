package social.message.streaming.function;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import social.message.streaming.entity.SentimentRow;

public  class FormatAsText2Fn extends SimpleFunction<KV<Integer, SentimentRow>, String> {

  @Override
  public String apply(KV<Integer, SentimentRow> input) {
    return input.getKey() + ": " + toTextSentimentRow(input.getValue());

  }

  private static String toTextSentimentRow(SentimentRow input) {
    StringBuilder sb = new StringBuilder();
    String windowText = String.format("Window %s - %s, ", input.getWindowTiming().getStartTime().toString(),
        input.getWindowTiming().getEndTime().toString());
    sb.append(windowText);
    sb.append("Negative: ").append(input.getLabelCounter().getNegativeCount());
    sb.append(" Positive: ").append(input.getLabelCounter().getPositiveCount());
    sb.append(" Neutral: ").append(input.getLabelCounter().getNeutralCount());
    return sb.toString();
  }
}