package social.message.streaming.util;

import social.message.streaming.dto.Sentiment;

import java.util.Random;

public final class DetectSentimentUtil {

    private DetectSentimentUtil(){}

    public static Sentiment detectSentiment(String message){
        return Sentiment.builder().score(new Random().nextDouble()).build();
    }

}
