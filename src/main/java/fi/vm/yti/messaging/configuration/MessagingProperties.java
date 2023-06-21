package fi.vm.yti.messaging.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("")
@Component
@Validated
public class MessagingProperties {

    private String defaultLanguage;

    public String getDefaultLanguage() {
        return this.defaultLanguage != null ? this.defaultLanguage : "en";
    }

    public void setDefaultLanguage(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
    }
}
