package social.message.streaming.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import social.message.streaming.type.SentimentLabel;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CompanySentiment implements Serializable {
    private Integer companyId;
    private SentimentLabel sentimentLabel;
}
