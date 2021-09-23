package social.message.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import social.message.streaming.tranform.SinkRowToBigTable;
import social.message.streaming.function.DetectCompanyLabelFn;
import social.message.streaming.function.FormatAsTextFn;
import social.message.streaming.function.MapTopicToSentimentLabelFn;
import social.message.streaming.util.ReadMessage;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.tranform.AggregateSentimentLabels;

public class App {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        PCollection<SentimentMessage> sentimentMessagePC = ReadMessage.fromPubSub(p)
            .apply(ParDo.of(DetectCompanyLabelFn.of()));

        //apply window
        final PCollection<SentimentMessage> windowedMessages = sentimentMessagePC.apply(
            String.join("_", String.valueOf(2), "Minutes", "Window"),
            assignWindows(2, 48));

        //write to file
        windowedMessages.apply(MapElements.via(new FormatAsTextFn()));

        //group by topic
        windowedMessages.apply(ParDo.of(MapTopicToSentimentLabelFn.of()))
            .apply(AggregateSentimentLabels.of())
            .apply(new SinkRowToBigTable());

        p.run();

    }


    public static PCollection<SentimentMessage> windowing(final PCollection<SentimentMessage> input) {
        return input.apply(
            String.join("_", String.valueOf(2), "Minutes", "Window"),
            assignWindows(2, 48));
    }

    public static <E> Window<E> assignWindows(
        final int windowSizeMinutes, final int allowLatenessHours) {
        return Window.<E>into(FixedWindows.of(Duration.standardMinutes(windowSizeMinutes)))
            .triggering(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(Duration.standardHours(allowLatenessHours))
            .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
            .discardingFiredPanes();
    }
    
}
