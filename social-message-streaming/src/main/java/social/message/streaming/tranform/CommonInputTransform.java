package social.message.streaming.tranform;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneId;
import social.message.streaming.function.DetectLanguageFn;
import social.message.streaming.dto.SentimentMessage;

@RequiredArgsConstructor(staticName = "of")
public class CommonInputTransform
    extends PTransform<PCollection<String>, PCollection<SentimentMessage>> {

  @Override
  public PCollection<SentimentMessage> expand(final PCollection<String> input) {
    PCollection<SentimentMessage> messages =
        input.apply(JsonToSentimentMessage.of())
        .apply(ParDo.of(new DetectLanguageFn()))

        /*.apply(ParDo.of(new DoFn<SentimentMessage, SentimentMessage>() {
          //apply outputWithTimestamp
          @ProcessElement
          public void processElement(@Element SentimentMessage element, OutputReceiver<SentimentMessage> out) {
                Instant logTimeStamp = extractTimestamp(element.getPublishedAt());
                out.outputWithTimestamp(element, logTimeStamp);
          }
        }))*/
        ;
    return messages;
  }

  private Instant extractTimestamp(final LocalDateTime dateTime) {
    return new Instant(dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
  }

  @Override
  protected String getKindString() {
    return "ParseEnrichFilterMessages";
  }
}
