package social.message.streaming.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import social.message.streaming.dto.CompanySentiment;
import social.message.streaming.util.PublishedAtSerializer;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@AllArgsConstructor
public class SentimentMessage implements Serializable {

  private String id;
  private String content;
  @JsonProperty("published_at")
  @JsonFormat(
      pattern =
          "[yyyy][/][-][.]MM[/][-][.]dd[/][-][.][yyyy]['T'][ ][H][HH]:mm[:ss][.][SSSSSS][SSS]['Z']"
  )
  @JsonSerialize(using = PublishedAtSerializer.class)
  @JsonDeserialize(using = LocalDateTimeDeserializer.class)
  private LocalDateTime publishedAt;

  private String lang;

  @JsonProperty("company_sentiment")
  private List<CompanySentiment> companySentiment;

}
