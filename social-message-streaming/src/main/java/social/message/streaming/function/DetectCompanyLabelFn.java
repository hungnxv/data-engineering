package social.message.streaming.function;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.beanutils.BeanUtils;
import social.message.streaming.dto.CompanySentiment;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.util.DetectCompanySentimentUtil;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Slf4j
@AllArgsConstructor(staticName = "of")
public class DetectCompanyLabelFn extends DoFn<SentimentMessage, SentimentMessage> {

    @ProcessElement
    public void processElement(ProcessContext context) throws InvocationTargetException, IllegalAccessException {
        final SentimentMessage input = context.element();
        List<CompanySentiment> companySentiments = DetectCompanySentimentUtil.detectCompanySentiment(input.getContent());
        SentimentMessage sentimentMessage = new SentimentMessage();
        BeanUtils.copyProperties(sentimentMessage, input);
        sentimentMessage.setCompanySentiment(companySentiments);
        for (CompanySentiment companySentiment: companySentiments) {
            log.info("Company {}, label {}", companySentiment.getCompanyId(), companySentiment.getSentimentLabel().getLabel());
        }
        context.output(sentimentMessage);
    }
    
}
