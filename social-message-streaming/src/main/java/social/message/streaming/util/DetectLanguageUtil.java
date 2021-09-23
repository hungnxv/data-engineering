package social.message.streaming.util;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;

public final class DetectLanguageUtil {

    private DetectLanguageUtil(){}

    public static Language detectLanguage(String message){
        final LanguageDetector detector = LanguageDetectorBuilder.fromLanguages(Language.ENGLISH, Language.VIETNAMESE).build();
        return detector.detectLanguageOf(message);
    }





}
