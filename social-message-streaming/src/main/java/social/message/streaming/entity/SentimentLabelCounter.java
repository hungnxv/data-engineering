package social.message.streaming.entity;

import com.google.bigtable.v2.Mutation;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;
import social.message.streaming.type.SentimentLabel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@DefaultCoder(SentimentLabelCounter.Coder.class)
public class SentimentLabelCounter implements BigTableMutableRow {
  private static final long serialVersionUID = 567611386612414891L;

  public static final String SENTIMENT_FAMILY = "sentiment";
  public static final String NEGATIVE_COL = "negative";
  public static final String NEUTRAL_COL = "neutral";
  public static final String POSITIVE_COL = "positive";
  public static final String ATTENTION_COL = "total";

  private int negativeCount = 0;
  private int neutralCount = 0;
  private int positiveCount = 0;

  public static SentimentLabelCounter of(
      final int negativeCount, final int neutralCount, final int positiveCount) {
    return new SentimentLabelCounter(negativeCount, neutralCount, positiveCount);
  }

  public SentimentLabelCounter(
      final int negativeCount, final int neutralCount, final int positiveCount) {
    this.negativeCount = negativeCount;
    this.neutralCount = neutralCount;
    this.positiveCount = positiveCount;
  }

  public long getTotal() {
    return negativeCount + neutralCount + positiveCount;
  }

  public void increaseNegativeCount() {
    this.negativeCount++;
  }

  public void increaseNeutralCount() {
    this.neutralCount++;
  }

  public void increasePositiveCount() {
    this.positiveCount++;
  }

  /** Mutable counting of the given {@link SentimentLabel} */
  public SentimentLabelCounter count(final SentimentLabel label) {
    switch (label) {
      case POSITIVE:
        increasePositiveCount();
        break;
      case NEUTRAL:
        increaseNeutralCount();
        break;
      case NEGATIVE:
        increaseNegativeCount();
        break;
      default:
        break;
    }
    return this;
  }

  public void increaseBy(final SentimentLabelCounter other) {
    this.positiveCount += other.positiveCount;
    this.negativeCount += other.negativeCount;
    this.neutralCount += other.neutralCount;
  }

  public SentimentLabelCounter duplicate() {
    return SentimentLabelCounter.of(this.negativeCount, this.neutralCount, this.positiveCount);
  }

  @Override
  public List<Mutation> toMutations(final Instant timestamp) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(buildSetMutation(SENTIMENT_FAMILY, NEGATIVE_COL, getNegativeCount()));
    mutations.add(buildSetMutation(SENTIMENT_FAMILY, NEUTRAL_COL, getNeutralCount()));
    mutations.add(buildSetMutation(SENTIMENT_FAMILY, POSITIVE_COL, getPositiveCount()));
    mutations.add(buildSetMutation(SENTIMENT_FAMILY, ATTENTION_COL, getTotal()));
    return mutations;
  }

  @NoArgsConstructor(staticName = "of")
  public static final class Coder extends AtomicCoder<SentimentLabelCounter> {
    private static final long serialVersionUID = 7985453110014092844L;

    private static final VarIntCoder INT_CODER = VarIntCoder.of();

    @Override
    public void encode(final SentimentLabelCounter value, final OutputStream outStream)
        throws IOException {
      checkNotNull(
          value, "Cannot encode a null " + SentimentLabelCounter.class.getSimpleName() + "!");
      INT_CODER.encode(value.negativeCount, outStream);
      INT_CODER.encode(value.neutralCount, outStream);
      INT_CODER.encode(value.positiveCount, outStream);
    }

    @Override
    public SentimentLabelCounter decode(final InputStream inStream) throws IOException {
      final SentimentLabelCounter counter = new SentimentLabelCounter();
      counter.negativeCount = INT_CODER.decode(inStream);
      counter.neutralCount = INT_CODER.decode(inStream);
      counter.positiveCount = INT_CODER.decode(inStream);
      return counter;
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(final SentimentLabelCounter value) {
      return true;
    }

    @Override
    public long getEncodedElementByteSize(final SentimentLabelCounter value) throws CoderException {
      if (value == null) {
        throw new CoderException(
            "Cannot encode a null " + SentimentLabelCounter.class.getSimpleName());
      }
      return VarInt.getLength(value.negativeCount)
          + VarInt.getLength(value.neutralCount)
          + VarInt.getLength(value.positiveCount);
    }

    @SuppressWarnings("unused")
    public static org.apache.beam.sdk.coders.CoderProvider getCoderProvider() {
      return new CoderProvider();
    }
  }

  public static final class CoderProvider extends org.apache.beam.sdk.coders.CoderProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> org.apache.beam.sdk.coders.Coder<T> coderFor(
        final TypeDescriptor<T> typeDescriptor,
        final List<? extends org.apache.beam.sdk.coders.Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!SentimentLabelCounter.class.isAssignableFrom(typeDescriptor.getRawType())) {
        throw new CannotProvideCoderException(
            "Only provide coder for " + SentimentLabelCounter.class.getSimpleName() + "!");
      }
      return (org.apache.beam.sdk.coders.Coder<T>) SentimentLabelCounter.Coder.of();
    }
  }
}
