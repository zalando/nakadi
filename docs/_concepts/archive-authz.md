# Authorization for Archiving Everything

## Overview

Organisations may wish to archive all the data that passes through Nakadi, for example in a data lake, using one or several applications that consume all events from all event types. Our experience has been that it is cumbersome, confusing, and error-prone, to ensure that event types that restrict READ allow the archiving application(s) to read their events. Changes in the archiving application also require modification of the authorization sections of all protected event types, which takes time and is confusing for event type owners.

This concept includes 2 possible solutions. Once a solution is chosen, the concept will be updated accordingly.

## Requirements

This concept aims to satisfy the following requirements:

1. Archiving applications can read from all event types, without exception. They cannot write nor administer event types, unless specified for individual event types;
2. Event type owners should not be able to accidentally forbid the archiving applications from reading events in an event type;
3. Event type owners who do not wish for their events to be read by the archiving applications should get in touch with the administrators of the archiving applications;
4. Event type owners should be made aware that the events published to their event type will be archived;
5. Only administrators can update the list of archiving applications. This operation should be simple.

The ability to exclude some event types from archival is not discussed in this concept.

## Solution 1: add all archiving applications to the list of authorized event types

The most simple solution is to add the archiving applications to the list of authorized event types:

- If an event type does not have an authorization section, do nothing;
- If an event type has an authorization section, add the list of archiving applications to the read section;
- If an event type is updated and has an authorization section, make sure that the archiving applications cannot be removed from the read section.

Nakadi will have to know the list of archiving applications; to do this, a new endpoint will be created:
```
POST /settings/resources/archivers
```
and its complement:
```
GET /settings/resources/archivers
```

These endpoints will be accessible to administrators with ADMIN rights only.

If the list of authorized applications is updated, all event types with an authorization section will be updated accordingly

### List of Tickets (to be created)

- Create new endpoints
- Update all event types with authorization when the list of archivers is updated;
- Add archiving applications to new event types, if there is an authorization section;
- Forbid updates of event types that remove archiving applications.

### Pros

- Relatively easy to implement: no changes to the authorization logic;
- Makes it obvious which applications can read from the event type.

### Cons

- Confusing for users: they are not able to remove some applications from the readers, and there is nothing that marks them as archiving applications;
- Updating the list of archiving applications could be cumbersome, as it may require updating a lot of event types.

## Solution 2: a new resource for archiving applications

In this solution, we create a new resource, `archive`, for which applications can only get READ permissions (write and admin must be empty).

When a client tries to read from an event type with an authorization section, if the client is not in the list of clients authorized to read from the event type, Nakadi will check if the client is in the list of archiving applications.

Nakadi will have to know the list of archiving applications; to do this, a new endpoint will be created:
```
POST /settings/resources/archivers
```
and its complement:
```
GET /settings/resources/archivers
```

These endpoints will be accessible to administrators with ADMIN rights only.

If the list of authorized applications is updated, event types with an authorization section do not need to be updated accordingly. However, all consumers (or at least those whose ID have been removed from the list) will have to be disconnected.

To make it clear that archiving applications are able to read from all event types, a warning will be included to the response for each successful creation of a new event type, or update of an existing event type.

### List of tickets (to be created)

- Create new endpoints;
- Add check for archiving applications when consumers connect (HiLA only as LoLA is being deprecated);
- Disconnect archiving applications that are removed from the list of archivers, when the list of archivers is updated;
- Add warning to response body when new event types are created and existing event types are updated.

### Pros

- Cleaner than solution 1;
- Avoids confusing users: the archiving applications do not appear in the list of authorized readers;
- Does not require updating all event types when the list is updated.

### Cons

- A little more implementation work, as there is a small change to the authorization logic for consumers;
- It is less obvious that archiving applications can read from the event type, but the warning as well as organization-wide policies can mitigate this.