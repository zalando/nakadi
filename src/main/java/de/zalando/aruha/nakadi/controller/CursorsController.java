package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.service.CursorsCommitService;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.Problem;

import java.util.List;

import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class CursorsController {

    private final SubscriptionDbRepository subscriptionRepository;
    private final TopicRepository topicRepository;
    private final CursorsCommitService cursorsCommitService;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public CursorsController(final SubscriptionDbRepository subscriptionRepository,
                             final TopicRepository topicRepository,
                             final CursorsCommitService cursorsCommitService,
                             final FeatureToggleService featureToggleService) {
        this.subscriptionRepository = subscriptionRepository;
        this.topicRepository = topicRepository;
        this.cursorsCommitService = cursorsCommitService;
        this.featureToggleService = featureToggleService;
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PUT)
    public ResponseEntity<?> commitCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                           @RequestBody final List<Cursor> cursors,
                                           final NativeWebRequest request) {

        if (!featureToggleService.isFeatureEnabled("high_level_api")) {
            return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        }
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            final String eventType = subscription.getEventTypes().iterator().next();

            topicRepository.validateCommitCursors(eventType, cursors);

            boolean allCommitted = true;
            for (final Cursor cursor : cursors) {
                allCommitted = allCommitted && cursorsCommitService.commitCursor(subscriptionId, eventType, cursor);
            }
            return allCommitted ? ok().build() : noContent().build();

        } catch (final NoSuchSubscriptionException | ServiceUnavailableException e) {
            return create(e.asProblem(), request);
        } catch (InvalidCursorException e) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
        }
    }
}
