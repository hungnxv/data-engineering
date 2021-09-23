package social.message.streaming.function;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import social.message.streaming.dto.CompanySentiment;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.type.SentimentLabel;

import java.util.Collection;

@AllArgsConstructor(staticName = "of")
public class MapTopicToSentimentLabelFn
    extends DoFn<SentimentMessage, KV<Integer, SentimentLabel>> {
  private static final long serialVersionUID = -998954792361613773L;

  @ProcessElement
  public void processElement(final ProcessContext context) {
    final Collection<CompanySentiment> companySentiments =
        context.element().getCompanySentiment();

    for (final CompanySentiment company : companySentiments) {
      context.output(KV.of(company.getCompanyId(), company.getSentimentLabel()));
    }
  }
}
