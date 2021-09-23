package social.message.streaming.function;

import org.apache.beam.sdk.transforms.Combine;
import social.message.streaming.entity.SentimentLabelCounter;
import social.message.streaming.type.SentimentLabel;

public class LabelsCombineFn
    extends Combine.CombineFn<SentimentLabel, SentimentLabelCounter, SentimentLabelCounter> {
    private static final long serialVersionUID = 1561312097062716047L;

    @Override
    public SentimentLabelCounter createAccumulator() {
        return new SentimentLabelCounter();
    }

    @Override
    public SentimentLabelCounter addInput(
        final SentimentLabelCounter counter, final SentimentLabel label) {
        return counter.count(label);
    }

    @Override
    public SentimentLabelCounter mergeAccumulators(final Iterable<SentimentLabelCounter> counters) {
        final SentimentLabelCounter result = new SentimentLabelCounter();
        for (final SentimentLabelCounter counter : counters) {
            result.increaseBy(counter);
        }
        return result;
    }

    @Override
    public SentimentLabelCounter extractOutput(final SentimentLabelCounter counter) {
        return counter;
    }
}
