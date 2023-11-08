package fi.vm.yti.messaging.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import fi.vm.yti.messaging.dto.IntegrationResourceDTO;
import fi.vm.yti.messaging.dto.IntegrationResponseDTO;
import fi.vm.yti.messaging.dto.ResourceDTO;
import fi.vm.yti.messaging.service.ContainerNameService;
import fi.vm.yti.messaging.service.IntegrationService;
import fi.vm.yti.messaging.service.ResourceService;
import static fi.vm.yti.messaging.api.ApiConstants.*;
import static fi.vm.yti.messaging.util.ApplicationUtils.getApplicationByType;

@Service
public class ContainerNameServiceImpl implements ContainerNameService {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerNameServiceImpl.class);

    private final Map<String, Map<String, String>> containerPrefLabels;
    private final IntegrationService integrationService;
    private final ResourceService resourceService;

    @Inject
    public ContainerNameServiceImpl(final IntegrationService integrationService,
                                    final ResourceService resourceService) {
        containerPrefLabels = new HashMap<>();
        this.resourceService = resourceService;
        this.integrationService = integrationService;
    }

    @Scheduled(cron = "0 0 0 * * *", zone = "Europe/Ljubljana")
    public void refreshPrefLabels() {
        fetchAndCachePrefLabelsForContainers(APPLICATION_CODELIST);
        fetchAndCachePrefLabelsForContainers(APPLICATION_DATAMODEL);
        fetchAndCachePrefLabelsForContainers(APPLICATION_TERMINOLOGY);
        fetchAndCachePrefLabelsForContainers(APPLICATION_COMMENTS);
    }

    public void addPrefLabel(final IntegrationResourceDTO integrationResource) {
        final Map<String, String> prefLabel = integrationResource.getPrefLabel();
        if (prefLabel != null && !prefLabel.isEmpty()) {
            containerPrefLabels.put(integrationResource.getUri(), integrationResource.getPrefLabel());
        }
    }

    public void addPrefLabelToUriWithType(final String uri,
                                          final String type) {
        final String applicationIdentifier = getApplicationByType(type);
        final Set<String> uris = new HashSet<>();
        uris.add(uri);
        final IntegrationResponseDTO integrationResponse = integrationService.getIntegrationContainers(applicationIdentifier, uris);
        final List<IntegrationResourceDTO> integrationResources = integrationResponse.getResults();
        if (integrationResources != null && !integrationResources.isEmpty()) {
            integrationResources.forEach(this::addPrefLabel);
        }
    }

    public Map<String, String> getPrefLabel(final String uri) {
        final Map<String, String> prefLabel = this.containerPrefLabels.get(uri);
        if (prefLabel != null && !prefLabel.isEmpty()) {
            LOG.info("PrefLabel found for uri: " + uri);
            return prefLabel;
        } else {
            LOG.info("PrefLabel not found for uri: " + uri);
            return null;
        }
    }

    private void fetchAndCachePrefLabelsForContainers(final String applicationIdentifier) {
        final Set<ResourceDTO> containerResources = resourceService.getResourcesForApplication(applicationIdentifier);
        if (containerResources != null && !containerResources.isEmpty()) {
            final Set<String> containerUris = new HashSet<>();
            containerResources.forEach(container -> containerUris.add(container.getUri()));
            if (!containerUris.isEmpty()) {
                final IntegrationResponseDTO integrationResponse = integrationService.getIntegrationContainers(applicationIdentifier, containerUris);
                final List<IntegrationResourceDTO> integrationResources = integrationResponse.getResults();
                if (integrationResources != null && !integrationResources.isEmpty()) {
                    integrationResources.forEach(integrationResource -> {
                        final Map<String, String> prefLabel = integrationResource.getPrefLabel();
                        if (prefLabel != null) {
                            LOG.info("Found prefLabel for uri: " + integrationResource.getUri());
                            containerPrefLabels.put(integrationResource.getUri(), integrationResource.getPrefLabel());
                        }
                    });
                }
            }
        }
    }
}
