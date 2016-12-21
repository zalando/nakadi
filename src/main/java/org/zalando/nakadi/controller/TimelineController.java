package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.VersionedCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class TimelineController {

    public static class TimelineToCreate {
        private String storageId;

        public String getStorageId() {
            return storageId;
        }

        public void setStorageId(final String storageId) {
            this.storageId = storageId;
        }


        protected void fill(final Timeline src) {
            this.storageId = src.getStorage().getId();
        }
    }

    public static class TimelineView extends TimelineToCreate {
        private Integer timelineId;
        private Integer order;
        private String eventType;
        private StorageController.StorageView storage;
        private Date createdAt;
        private Date cleanupAt;
        private Date switchedAt;
        private Date freedAt;
        private List<Cursor> latestCursors;

        public Integer getTimelineId() {
            return timelineId;
        }

        public void setTimelineId(final Integer timelineId) {
            this.timelineId = timelineId;
        }

        public Integer getOrder() {
            return order;
        }

        public void setOrder(final Integer order) {
            this.order = order;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(final String eventType) {
            this.eventType = eventType;
        }

        public StorageController.StorageView getStorage() {
            return storage;
        }

        public void setStorage(final StorageController.StorageView storage) {
            this.storage = storage;
        }

        public Date getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(final Date createdAt) {
            this.createdAt = createdAt;
        }

        public Date getCleanupAt() {
            return cleanupAt;
        }

        public void setCleanupAt(final Date cleanupAt) {
            this.cleanupAt = cleanupAt;
        }

        public Date getSwitchedAt() {
            return switchedAt;
        }

        public void setSwitchedAt(final Date switchedAt) {
            this.switchedAt = switchedAt;
        }

        public Date getFreedAt() {
            return freedAt;
        }

        public void setFreedAt(final Date freedAt) {
            this.freedAt = freedAt;
        }

        public List<Cursor> getLatestCursors() {
            return latestCursors;
        }

        public void setLatestCursors(final List<Cursor> latestCursors) {
            this.latestCursors = latestCursors;
        }

        public static TimelineView build(final Timeline src) {
            if (null == src) {
                return null;
            }
            final TimelineView result = new TimelineView();
            result.fill(src);
            result.timelineId = src.getId();
            result.order = src.getOrder();
            result.storage = StorageController.StorageView.build(src.getStorage());
            result.createdAt = src.getCreatedAt();
            result.cleanupAt = src.getCleanupAt();
            result.switchedAt = src.getSwitchedAt();
            result.freedAt = src.getFreedAt();
            switch (src.getStorage().getType()) {
                case KAFKA:
                    break;
            }
            result.latestCursors = src.getLastPosition().stream().map(VersionedCursor::toCursor).collect(Collectors.toList());
            return result;
        }
    }

    private final TimelineService timelineService;

    @Autowired
    public TimelineController(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/timelines", method = RequestMethod.GET)
    public List<TimelineView> listTimelines(
            @PathVariable("eventTypeName") final String eventType) {
        return timelineService.listTimelines(eventType).stream().map(TimelineView::build).collect(Collectors.toList());
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/timelines", method = RequestMethod.POST)
    public TimelineView createTimeline(
            @PathVariable("eventTypeName") final String eventType,
            @RequestBody final TimelineToCreate timeline)
            throws InterruptedException, NoSuchEventTypeException, InternalNakadiException {
        return TimelineView.build(timelineService.createAndStartTimeline(eventType, timeline.storageId));
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/timelines/{timelineId}", method = RequestMethod.DELETE)
    public void deleteTimeline(
            @PathVariable("eventTypeName") final String eventType,
            @PathVariable("timelineId") final Integer timelineId) throws InterruptedException {
        timelineService.deleteTimeline(eventType, timelineId);
    }
}
