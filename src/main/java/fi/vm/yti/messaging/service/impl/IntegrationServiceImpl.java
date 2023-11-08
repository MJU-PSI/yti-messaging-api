package fi.vm.yti.messaging.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import fi.vm.yti.messaging.configuration.CodelistProperties;
import fi.vm.yti.messaging.configuration.CommentsProperties;
import fi.vm.yti.messaging.configuration.CustomObjectMapper;
import fi.vm.yti.messaging.configuration.DataModelProperties;
import fi.vm.yti.messaging.configuration.TerminologyProperties;
import fi.vm.yti.messaging.dto.ErrorModel;
import fi.vm.yti.messaging.dto.IntegrationResourceRequestDTO;
import fi.vm.yti.messaging.dto.IntegrationResponseDTO;
import fi.vm.yti.messaging.exception.NotFoundException;
import fi.vm.yti.messaging.exception.YtiMessagingException;
import fi.vm.yti.messaging.service.IntegrationService;
import static fi.vm.yti.messaging.api.ApiConstants.*;
import static fi.vm.yti.messaging.util.ApplicationUtils.TYPE_CODE;
import static org.assertj.core.util.DateUtil.now;
import static org.assertj.core.util.DateUtil.yesterday;

@Service
public class IntegrationServiceImpl implements IntegrationService {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrationServiceImpl.class);
    private static final String DATE_SUFFIX = "T07:00:00.000+0200";
    private static final String LANGUAGE_CODE_FI = "fi";

    private final CodelistProperties codelistProperties;
    private final DataModelProperties dataModelProperties;
    private final TerminologyProperties terminologyProperties;
    private final CommentsProperties commentsProperties;
    private final RestTemplate restTemplate;

    public IntegrationServiceImpl(final CodelistProperties codelistProperties,
                                  final DataModelProperties dataModelProperties,
                                  final TerminologyProperties terminologyProperties,
                                  final CommentsProperties commentsProperties,
                                  final RestTemplate restTemplate) {
        this.codelistProperties = codelistProperties;
        this.dataModelProperties = dataModelProperties;
        this.terminologyProperties = terminologyProperties;
        this.commentsProperties = commentsProperties;
        this.restTemplate = restTemplate;
    }

    public IntegrationResponseDTO getIntegrationContainers(final String applicationIdentifier,
                                                           final Set<String> containerUris) {
        return getIntegrationContainers(applicationIdentifier, containerUris, false, false);
    }

    public IntegrationResponseDTO getIntegrationContainers(final String applicationIdentifier,
                                                           final Set<String> containerUris,
                                                           final boolean fetchDateRangeChanges,
                                                           final boolean getLatest) {
        final String requestUrl = resolveIntegrationContainersRequestUrl(applicationIdentifier);
        LOG.info("Fetching integration containers from: " + requestUrl);
        final String requestBody = createContainerRequestBody(containerUris, fetchDateRangeChanges, getLatest);
        LOG.info("Fetching integration containers body: " + requestBody);
        final HttpEntity requestEntity = new HttpEntity<>(requestBody, createRequestHeaders());
        try {
            final ResponseEntity response = restTemplate.exchange(requestUrl, HttpMethod.POST, requestEntity, String.class);
            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                return parseIntegrationResponse(response);
            } else {
                throw new NotFoundException();
            }
        } catch (final Exception e) {
            LOG.error("Fetching integration containers failed for application: " + applicationIdentifier, e);
            throw new YtiMessagingException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Failed to fetch integration containers for application: " + applicationIdentifier));
        }
    }

    public IntegrationResponseDTO getIntegrationResources(final String applicationIdentifier,
                                                          final String containerUri,
                                                          final boolean fetchDateRangeChanges,
                                                          final boolean getLatest) {
        final String requestUrl = resolveIntegrationResourcesRequestUrl(applicationIdentifier);
        LOG.debug("Fetching integration resources from: " + requestUrl);
        final String requestBody = createResourcesRequestBody(applicationIdentifier, containerUri, fetchDateRangeChanges, getLatest);
        LOG.debug("Fetching integration resources body: " + requestBody);
        final HttpEntity requestEntity = new HttpEntity<>(requestBody, createRequestHeaders());
        try {
            final ResponseEntity response = restTemplate.exchange(requestUrl, HttpMethod.POST, requestEntity, String.class);
            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                return parseIntegrationResponse(response);
            } else {
                throw new NotFoundException();
            }
        } catch (final Exception e) {
            LOG.error("Fetching integration resources failed for application: " + applicationIdentifier, e);
            throw new YtiMessagingException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Failed to fetch integration resources for application: " + applicationIdentifier));
        }
    }

    private IntegrationResponseDTO parseIntegrationResponse(final ResponseEntity response) {
        final Object responseBody = response.getBody();
        LOG.debug("Fetching integration resources response: " + responseBody);
        if (responseBody != null) {
            try {
                final ObjectMapper mapper = new ObjectMapper();
                mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
                final String data = responseBody.toString();
                final IntegrationResponseDTO integrationResponse = mapper.readValue(data, new TypeReference<IntegrationResponseDTO>() {
                });
                Collections.sort(integrationResponse.getResults());
                return integrationResponse;
            } catch (final IOException e) {
                throw new YtiMessagingException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Failed to parse integration resources!"));
            }
        } else {
            throw new YtiMessagingException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Failed to parse integration resources!"));
        }
    }

    private HttpHeaders createRequestHeaders() {
        final HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.add("Content-Type", MediaType.APPLICATION_JSON);
        requestHeaders.add("Accept", MediaType.APPLICATION_JSON);
        return requestHeaders;
    }

    private String createContainerRequestBody(final Set<String> containerUris,
                                              final boolean fetchDateRangeChanges,
                                              final boolean getLatest) {
        final ObjectMapper mapper = new CustomObjectMapper();
        final IntegrationResourceRequestDTO integrationResourceRequest = new IntegrationResourceRequestDTO();
        integrationResourceRequest.setIncludeIncomplete(true);
        if (fetchDateRangeChanges) {
            setAfterAndBefore(integrationResourceRequest, getLatest);
        }
        if (containerUris != null && !containerUris.isEmpty()) {
            integrationResourceRequest.setUri(new ArrayList<>(containerUris));
        }
        integrationResourceRequest.setLanguage(LANGUAGE_CODE_FI);
        try {
            return mapper.writeValueAsString(integrationResourceRequest);
        } catch (final JsonProcessingException e) {
            throw new YtiMessagingException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Integration request body generation failed due to error: " + e.getMessage()));
        }
    }

    private String createResourcesRequestBody(final String applicationIdentifier,
                                              final String container,
                                              final boolean fetchDateRangeChanges,
                                              final boolean getLatest) {
        final ObjectMapper mapper = new CustomObjectMapper();
        final IntegrationResourceRequestDTO integrationResourceRequest = new IntegrationResourceRequestDTO();
        integrationResourceRequest.setIncludeIncomplete(true);
        if (fetchDateRangeChanges) {
            setAfterAndBefore(integrationResourceRequest, getLatest);
        }
        if (container != null && !container.isEmpty()) {
            final List<String> containerUris = new ArrayList<>();
            containerUris.add(container);
            integrationResourceRequest.setContainer(containerUris);
        }
        if (applicationIdentifier.equalsIgnoreCase(APPLICATION_CODELIST)) {
            integrationResourceRequest.setType(TYPE_CODE);
        }
        integrationResourceRequest.setLanguage(LANGUAGE_CODE_FI);
        integrationResourceRequest.setPageFrom(0);
        integrationResourceRequest.setPageSize(RESOURCES_PAGE_SIZE);
        try {
            return mapper.writeValueAsString(integrationResourceRequest);
        } catch (final JsonProcessingException e) {
            throw new YtiMessagingException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Integration request body generation failed due to error: " + e.getMessage()));
        }
    }

    private void setAfterAndBefore(final IntegrationResourceRequestDTO integrationResourceRequestDto,
                                   final boolean getLatest) {
        final TimeZone tz = TimeZone.getTimeZone("Europe/Ljubljana");
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(tz);
        final String after = df.format(yesterday()) + DATE_SUFFIX;
        integrationResourceRequestDto.setAfter(after);
        if (!getLatest) {
            final String before = df.format(now()) + DATE_SUFFIX;
            integrationResourceRequestDto.setBefore(before);
        }
    }

    private String resolveIntegrationRequestUrl(final String applicationIdentifier,
                                                final String endPoint) {
        switch (applicationIdentifier) {
            case APPLICATION_CODELIST:
                return resolveIntegrationRequestUrl(codelistProperties.getPublicUrl(), PATH_CODELIST_API, endPoint);
            case APPLICATION_DATAMODEL:
                return resolveIntegrationRequestUrl(dataModelProperties.getPublicUrl(), PATH_DATAMODEL_API, endPoint);
            case APPLICATION_TERMINOLOGY:
                return resolveIntegrationRequestUrl(terminologyProperties.getPublicUrl(), PATH_TERMINOLOGY_API, endPoint);
            case APPLICATION_COMMENTS:
                return resolveIntegrationRequestUrl(commentsProperties.getPublicUrl(), PATH_COMMENTS_API, endPoint);
            default:
                throw new YtiMessagingException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Unknown applicationIdentifier: " + applicationIdentifier));
        }
    }

    private String resolveIntegrationRequestUrl(final String publicUrl,
                                                final String applicationApiPath,
                                                final String endPoint) {
        return publicUrl + applicationApiPath + PATH_API + PATH_V1 + PATH_INTEGRATION + endPoint;
    }

    private String resolveIntegrationContainersRequestUrl(final String applicationIdentifier) {
        return resolveIntegrationRequestUrl(applicationIdentifier, PATH_CONTAINERS_API);
    }

    private String resolveIntegrationResourcesRequestUrl(final String applicationIdentifier) {
        return resolveIntegrationRequestUrl(applicationIdentifier, PATH_RESOURCES_API);
    }
}
