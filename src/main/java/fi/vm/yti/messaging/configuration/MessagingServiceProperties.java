package fi.vm.yti.messaging.configuration;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("application")
@Component
@Validated
public class MessagingServiceProperties {

    @NotNull
    private String contextPath;

    private String env;
    private String defaultLanguage;

    public String getEnv() {
        return env;
    }

    public void setEnv(final String env) {
        this.env = env;
    }

    public String getContextPath() {
        return contextPath;
    }

    public void setContextPath(final String contextPath) {
        this.contextPath = contextPath;
    }

    public String getDefaultLanguage() {
        return this.defaultLanguage != null ? this.defaultLanguage : "en";
    }

    public void setDefaultLanguage(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
    }
}
