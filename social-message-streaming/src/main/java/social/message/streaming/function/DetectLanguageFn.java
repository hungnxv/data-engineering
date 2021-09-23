package social.message.streaming.function;

import com.github.pemistahl.lingua.api.Language;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.beanutils.BeanUtils;
import social.message.streaming.dto.SentimentMessage;
import social.message.streaming.util.DetectLanguageUtil;

import java.lang.reflect.InvocationTargetException;

public class DetectLanguageFn extends DoFn<SentimentMessage, SentimentMessage> {

    @ProcessElement
    public void processElement(ProcessContext context) throws InvocationTargetException, IllegalAccessException {
        final SentimentMessage input = context.element();
        Language language = DetectLanguageUtil.detectLanguage(input.getContent());
        if(language.equals(Language.ENGLISH)){
            SentimentMessage sentimentMessage = new SentimentMessage();
            BeanUtils.copyProperties(sentimentMessage, input);
            sentimentMessage.setLang(language.getIsoCode639_1().name());
            context.output(sentimentMessage);
        }

    }
}
