package fi.vm.yti.messaging.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import javax.sql.DataSource;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.ajp.AjpNioProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.ErrorPage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.support.ResourceBundleMessageSource;

@Component
@Configuration
@EnableScheduling
@PropertySource(value = "classpath", ignoreResourceNotFound = true)
public class SpringAppConfig {

    private static final int CONNECTION_TIMEOUT = 30000;

    @Value(value = "${application.contextPath}")
    private String contextPath;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public TomcatServletWebServerFactory servletContainer(@Value("${tomcat.ajp.port:}") Integer ajpPort) throws UnknownHostException {
        final TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.setContextPath(contextPath);
        tomcat.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/notfound.html"));
        if (ajpPort != null) {
            final Connector ajpConnector = new Connector("AJP/1.3");
            ajpConnector.setPort(ajpPort);
            ajpConnector.setSecure(false);
            ajpConnector.setAllowTrace(false);
            ajpConnector.setScheme("http");
            ajpConnector.setProperty("allowedRequestAttributesPattern", ".*");

            AjpNioProtocol protocol= (AjpNioProtocol) ajpConnector.getProtocolHandler();
            protocol.setSecretRequired(false);
            protocol.setAddress(InetAddress.getByName("0.0.0.0"));

            tomcat.addAdditionalTomcatConnectors(ajpConnector);
        }
        return tomcat;
    }

    @ConfigurationProperties(prefix = "hikari")
    @Bean
    public DataSource dataSource() {
        return new HikariDataSource();
    }

    @Bean
    public ResourceBundleMessageSource messageSource() {

        var source = new ResourceBundleMessageSource();
        source.setBasenames("messages/labels");
        source.setUseCodeAsDefaultMessage(true);
        source.setDefaultEncoding("UTF-8");

        return source;
    }

    @Bean
    ClientHttpRequestFactory httpRequestFactory() {
        final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECTION_TIMEOUT);
        requestFactory.setReadTimeout(CONNECTION_TIMEOUT);
        return requestFactory;
    }

    @Bean
    RestTemplate restTemplate() {
        final RestTemplate restTemplate = new RestTemplate(httpRequestFactory());
        restTemplate.getMessageConverters()
            .add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
        return restTemplate;
    }
}
