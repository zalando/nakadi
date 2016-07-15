package de.zalando.aruha.nakadi.controller;

import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.SubscriptionBase;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import javax.ws.rs.core.Response;
import java.util.List;


@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionController.class);

    private final SubscriptionDbRepository subscriptionRepository;

    private final EventTypeRepository eventTypeRepository;

    private final FeatureToggleService featureToggleService;

    @Autowired
    public SubscriptionController(final SubscriptionDbRepository subscriptionRepository,
                                  final EventTypeRepository eventTypeRepository,
                                  final FeatureToggleService featureToggleService) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.featureToggleService = featureToggleService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final SubscriptionBase subscriptionBase,
                                                     final Errors errors,
                                                     final NativeWebRequest request) {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.HIGH_LEVEL_API)) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        try {
            return createSubscription(subscriptionBase, request);
        } catch (final DuplicatedSubscriptionException e) {
            try {
                return new ResponseEntity<>(getExistingSubscription(subscriptionBase), HttpStatus.OK);
            } catch (final NakadiException ex) {
                LOG.error("Error occurred during fetching existing subscription", ex);
                return Responses.create(Response.Status.INTERNAL_SERVER_ERROR, ex.getProblemMessage(), request);
            }
        } catch (final InternalNakadiException e) {
            LOG.error("Error occurred during subscription creation", e);
            return Responses.create(e.asProblem(), request);
        }
    }

    private ResponseEntity<?> createSubscription(final SubscriptionBase subscriptionBase, final NativeWebRequest request)
            throws InternalNakadiException, DuplicatedSubscriptionException {
        final List<String> noneExistingEventTypes = checkExistingEventTypes(subscriptionBase);
        if (!noneExistingEventTypes.isEmpty()) {
            final String errorMessage = createErrorMessage(noneExistingEventTypes);
            LOG.debug(errorMessage);
            return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, errorMessage, request);
        }

        // generate subscription id and try to create subscription in DB
        final Subscription subscription = subscriptionRepository.createSubscription(subscriptionBase);
        return new ResponseEntity<>(subscription, HttpStatus.CREATED);
    }

    private List<String> checkExistingEventTypes(final SubscriptionBase subscriptionBase) throws InternalNakadiException {
        final List<String> noneExistingEventTypes = Lists.newArrayList();
        for (final String etName : subscriptionBase.getEventTypes()) {
            try {
                eventTypeRepository.findByName(etName);
            } catch (NoSuchEventTypeException e) {
                noneExistingEventTypes.add(etName);
            }
        }
        return noneExistingEventTypes;
    }

    private String createErrorMessage(final List<String> noneExistingEventTypes) {
        return new StringBuilder()
                .append("Failed to create subscription, event type(s) not found: '")
                .append(StringUtils.join(noneExistingEventTypes, "','"))
                .append("'").toString();
    }

    private Subscription getExistingSubscription(final SubscriptionBase subscriptionBase)
            throws NoSuchSubscriptionException, InternalNakadiException {
        // if the subscription with such parameters already exists - return it instead of creating a new one
        return subscriptionRepository.getSubscription(
                subscriptionBase.getOwningApplication(),
                subscriptionBase.getEventTypes(),
                subscriptionBase.getConsumerGroup());
    }

}
