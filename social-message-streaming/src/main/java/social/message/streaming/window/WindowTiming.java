package social.message.streaming.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

import java.io.Serializable;

@AllArgsConstructor
@Data(staticConstructor = "of")
public class WindowTiming implements Serializable {

  private Instant startTime;
  private Instant endTime;

  public WindowTiming() {
    this.startTime = Instant.now();
    this.endTime = Instant.now();
  }

  public long getDurationInMillis() {
    return endTime.getMillis() - startTime.getMillis();
  }

  public WindowTiming duplicate() {
    return new WindowTiming(startTime, endTime);
  }

  public IntervalWindow toIntervalWindow() {
    return new IntervalWindow(startTime, endTime);
  }

  public static WindowTiming of(final Instant startTime, final Instant endTime) {
    return new WindowTiming(startTime, endTime);
  }

  public static WindowTiming of(final IntervalWindow intervalWindow) {
    return new WindowTiming(intervalWindow.start(), intervalWindow.end());
  }
}
