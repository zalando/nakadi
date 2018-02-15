# Authorization for Archiving Everything

## Overview

Organisations may wish to archive all the data that passes through Nakadi, for example in a data lake, using one or several applications that consume all events from all event types. Our experience has been that it is cumbersome, confusing, and error-prone, to ensure that event types that restrict READ allow the archiving application(s) to read their events. Changes in the archiving application also require modification of the authorization sections of all protected event types, which takes time and is confusing for event type owners.

This concept outlines a temporary solution to allow a set of applications to read from all event types. The solution described here represents technical debt that will have to be repaid later, which will involve a re-design of authorization in Nakadi.

## Requirements

This concept aims to satisfy the following requirements:

1. Archiving applications can read from all event types, without exception. They cannot write nor administer event types, unless specified for individual event types;
2. Event type owners should not be able to accidentally forbid the archiving applications from reading events in an event type;
3. Event type owners who do not wish for their events to be read by the archiving applications should get in touch with the administrators of the archiving applications;
4. Event type owners should be made aware that the events published to their event type will be archived;

The ability to exclude some event types from archival is not discussed in this concept.

## Solution: a new resource for archiving applications

In this solution, we create a new resource, `archive`, for which applications can only get READ permissions (write and admin must be empty).

When a client tries to read from an event type with an authorization section, if the client is not in the list of clients authorized to read from the event type, Nakadi will check if the client is in the list of archiving applications.

Nakadi will have to know the list of archiving applications; they will be added to the database, with the resource name 'all_data_access'

To make it clear that archiving applications are able to read from all event types, a warning will be included to the response for each successful creation of a new event type, or update of an existing event type.

### List of tickets (to be created)

- Add check for archiving applications when consumers connect (HiLA only as LoLA is being deprecated);
- Add warning to response headers when new event types are created and existing event types are updated.
