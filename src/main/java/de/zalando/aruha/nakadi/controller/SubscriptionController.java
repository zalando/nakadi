package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
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

import javax.validation.Valid;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionController.class);

    private final SubscriptionDbRepository subscriptionRepository;

    private final EventTypeRepository eventTypeRepository;

    @Autowired
    public SubscriptionController(final SubscriptionDbRepository subscriptionRepository,
                                  final EventTypeRepository eventTypeRepository) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createOrGetSubscription(@Valid @RequestBody final Subscription subscription,
                                                     final Errors errors, final NativeWebRequest nativeWebRequest) {
        if (errors.hasErrors()) {
            return create(new ValidationProblem(errors), nativeWebRequest);
        }

        try {
            // check that event types exist
            final List<String> noneExistingEventTypes = newArrayList();
            for (final String etName : subscription.getEventTypes()) {
                try {
                    eventTypeRepository.findByName(etName);
                } catch (NoSuchEventTypeException e) {
                    noneExistingEventTypes.add(etName);
                }
            }
            if (!noneExistingEventTypes.isEmpty()) {
                final String errorMessage = "Failed to create subscription, event type(s) not found: '" +
                        StringUtils.join(noneExistingEventTypes, "','") + "'";
                LOG.debug(errorMessage);
                return create(NOT_FOUND, errorMessage, nativeWebRequest);
            }

            // generate subscription id and try to create subscription in DB
            subscription.setId(UUID.randomUUID().toString());
            subscriptionRepository.saveSubscription(subscription);
            return new ResponseEntity<>(subscription, HttpStatus.CREATED);

        } catch (final DuplicatedSubscriptionException e) {
            try {
                // if the subscription with such parameters already exists - return it instead of creating a new one
                final Subscription existingSubscription = subscriptionRepository.getSubscription(
                        subscription.getOwningApplication(), subscription.getEventTypes(),
                        subscription.getConsumerGroup());
                return new ResponseEntity<>(existingSubscription, HttpStatus.OK);

            } catch (final NakadiException ex) {
                LOG.error("Error occurred during fetching existing subscription", ex);
                return create(INTERNAL_SERVER_ERROR, ex.getProblemMessage(), nativeWebRequest);
            }
        } catch (final InternalNakadiException e) {
            LOG.error("Error occurred during subscription creation", e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

}
