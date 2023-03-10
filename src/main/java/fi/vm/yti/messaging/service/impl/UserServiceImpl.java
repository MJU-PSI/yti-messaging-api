package fi.vm.yti.messaging.service.impl;

import java.util.*;

import javax.inject.Inject;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import fi.vm.yti.messaging.dao.ResourceDao;
import fi.vm.yti.messaging.dao.UserDao;
import fi.vm.yti.messaging.dto.ResourceDTO;
import fi.vm.yti.messaging.dto.UserDTO;
import fi.vm.yti.messaging.entity.Resource;
import fi.vm.yti.messaging.entity.User;
import fi.vm.yti.messaging.exception.NotFoundException;
import fi.vm.yti.messaging.service.DtoMapperService;
import fi.vm.yti.messaging.service.UserService;

@Service
public class UserServiceImpl implements UserService {

    private final UserDao userDao;
    private final ResourceDao resourceDao;
    private final DtoMapperService dtoMapperService;

    @Inject
    public UserServiceImpl(final UserDao userDao,
                           final ResourceDao resourceDao,
                           final DtoMapperService dtoMapperService) {
        this.userDao = userDao;
        this.resourceDao = resourceDao;
        this.dtoMapperService = dtoMapperService;
    }

    @Transactional
    public UserDTO findById(final UUID userId) {
        return dtoMapperService.mapUser(userDao.findById(userId));
    }

    @Transactional
    public Set<UserDTO> findAll() {
        return dtoMapperService.mapUsers(userDao.findAll());
    }

    @Transactional
    public UserDTO setSubscriptionType(final UUID userId,
                                       final String subscriptionType) {
        return dtoMapperService.mapUser(userDao.setSubscriptionType(userId, subscriptionType));
    }

    @Transactional
    public UserDTO getOrCreateUser(final UUID userId) {
        return dtoMapperService.mapUser(userDao.getOrCreateUser(userId));
    }

    @Transactional
    public ResourceDTO addResourceToUser(final String uri,
                                         final String type,
                                         final UUID userId) {
        final User user = userDao.getOrCreateUser(userId);
        Set<Resource> resources = user.getResources();
        if (resources == null) {
            resources = new HashSet<>();
        }
        final Resource resource = resourceDao.getOrCreateResource(uri, type);
        resources.add(resource);
        userDao.save(user);
        return dtoMapperService.mapResource(resource);
    }

    @Transactional
    public ResourceDTO getSubscription(final String uri,
                                       final UUID userId) {
        final User user = userDao.getUser(userId);
        if (user != null) {
            final Set<Resource> resources = user.getResources();
            if (resources != null) {
                for (final Resource resource : resources) {
                    if (resource.getUri().equalsIgnoreCase(uri)) {
                        return dtoMapperService.mapResource(resource);
                    }
                }
            }
        }
        return null;
    }

    @Transactional
    public ResourceDTO deleteResourceFromUser(final String uri,
                                              final UUID userId) {
        final User user = userDao.findById(userId);
        if (user != null && uri != null) {
            final Set<Resource> resources = user.getResources();
            List<Resource> resourceToBeDeleted = new ArrayList<>();
            List<String> uris = Arrays.asList(uri.split(","));

            for(String u : uris) {
                for (final Resource resource : resources) {
                    if (resource.getUri().equalsIgnoreCase(u)) {
                        resourceToBeDeleted.add(resource);
                    }
                }
            }
            resources.removeAll(resourceToBeDeleted);
            user.setResources(resources);
            userDao.save(user);
            return dtoMapperService.mapResource(resourceToBeDeleted);
        } else {
            throw new NotFoundException();
        }
    }
}
