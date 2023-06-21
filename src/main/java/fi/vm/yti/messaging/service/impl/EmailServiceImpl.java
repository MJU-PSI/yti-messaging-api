package fi.vm.yti.messaging.service.impl;

import java.util.Locale;
import java.util.UUID;

import javax.inject.Inject;
import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import fi.vm.yti.messaging.configuration.MessagingProperties;
import fi.vm.yti.messaging.service.EmailService;
import fi.vm.yti.messaging.service.UserLookupService;
import static javax.mail.Message.RecipientType.TO;

@Service
public class EmailServiceImpl implements EmailService {

    private static final Logger LOG = LoggerFactory.getLogger(EmailServiceImpl.class);
    private final UserLookupService userLookupService;
    private final JavaMailSender javaMailSender;
    private final String adminEmail;
    
    private final MessagingProperties messagingProperties;

    @Autowired
    private MessageSource messageSource;

    @Inject
    public EmailServiceImpl(final UserLookupService userLookupService,
                            final JavaMailSender javaMailSender,
                            final MessagingProperties messagingProperties,
                            @Value("${admin.email}") String adminEmail) {
        this.userLookupService = userLookupService;
        this.javaMailSender = javaMailSender;
        this.messagingProperties = messagingProperties;
        this.adminEmail = adminEmail;
    }

    private static Address createAddress(final String emailAddress) {
        try {
            return new InternetAddress(emailAddress);
        } catch (final AddressException e) {
            LOG.error("createAddress failed for " + emailAddress);
            throw new RuntimeException(e);
        }
    }

    public void sendMail(final UUID userId,
                         final String message) {
        final String emailAddress = userLookupService.getUserEmailById(userId);
        try {
            if (emailAddress != null && !emailAddress.endsWith("localhost")) {
                LOG.info("Sending email to: " + userId);
                LOG.debug("Email message: " + message);
                final MimeMessage mail = javaMailSender.createMimeMessage();
                mail.setRecipient(TO, createAddress(emailAddress));
                mail.setFrom(createAddress(adminEmail));
                mail.setSender(createAddress(adminEmail));
                mail.setSubject(messageSource.getMessage("l33",null, Locale.forLanguageTag(messagingProperties.getDefaultLanguage())));
                mail.setContent(message, "text/html; charset=UTF-8");
                javaMailSender.send(mail);
            } else {
                LOG.info("Not sending e-mail to a localhost or removed user : " + emailAddress);
            }
        } catch (final Exception e) {
            LOG.error("Email sending failed due to " + e.getMessage(), e);
        }
    }
}
