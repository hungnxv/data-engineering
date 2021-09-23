package social.message.streaming.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.tranform.CommonInputTransform;

public final class ReadMessage {
    private static final String SUBSCRIPTION = "GOOGLE_PUBSUB_TOP";

    private ReadMessage(){}

    public static PCollection<SentimentMessage> fromPubSub(final Pipeline pipeline){

        return pipeline
            .apply(String.format("PubSub: %s", SUBSCRIPTION),
                PubsubIO.readStrings().withTimestampAttribute("published_at")
                .fromSubscription(SUBSCRIPTION)).apply(CommonInputTransform.of());
    }


}
