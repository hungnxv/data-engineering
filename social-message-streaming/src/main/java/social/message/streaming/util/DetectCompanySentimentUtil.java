package social.message.streaming.util;

import social.message.streaming.dto.CompanySentiment;
import social.message.streaming.type.SentimentLabel;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public final class DetectCompanySentimentUtil {

    private DetectCompanySentimentUtil(){}

    /**
     * get dummy company, would be replaced with trie and database
     * @param message
     * @return
     */
    public static List<CompanySentiment> detectCompanySentiment(String message){
        return Arrays.asList(getCompanySentiment(1), getCompanySentiment(2), getCompanySentiment(3), getCompanySentiment(4));
    }

    private static CompanySentiment getCompanySentiment(int companyId){
        return CompanySentiment.builder()
            .companyId(companyId)
            .sentimentLabel(getSentimentLabel()).build();
    }

    private static SentimentLabel getSentimentLabel(){
        Random rd = new Random();
        if(rd.nextBoolean())
            return SentimentLabel.NEGATIVE;
        else if(rd.nextBoolean())
            return SentimentLabel.POSITIVE;
        else
            return SentimentLabel.NEUTRAL;
    }

}
