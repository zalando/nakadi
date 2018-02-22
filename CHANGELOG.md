# Change Log
All notable changes to `Nakadi` will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [2.5.8] - 2018-02-22

### Added
- Provides optional warning header when creating or updating event types. The purpose is to warn users of the archival of all events.
- Applications with READ rights to all_data_access can read from all event types, regardless of the event types' 
authorization policy

### Changed
- Low-level API marked as deprecated

## [2.5.7] - 2018-02-15

### Changed
- Optimize subscription rebalance with inter-region zookeepers

### Fixed
- Server does not implement Problem JSON for auth errors

## [2.5.5] - 2018-01-23

### Changed
- Updated json-schema validation library

## [2.5.4] - 2018-01-18

### Added
- Allow to patch subscription cursors in case when they were not initialized

### Fixed
- Allow to update event types with recursion in schemas

### Changed
- Distinguish between employee token and service token in KPI events

## [2.5.2] - 2018-01-08

### Fixed
- Do not create connection to kafka if kafka services are not needed

## [2.5.1] - 2018-01-03

### Added
- Added publishing of subscription log events 

## [2.5.0] - 2017-12-22

### Added
- Nakadi collects event publishing KPI data 
- Nakadi collects event streaming KPI data

## [2.4.2] - 2017-12-21

### Added
- Allow changing event type category from NONE to BUSINESS

### Fixed
- Added timeout for subscription locking

## [2.4.1] - 2017-12-18

### Fixed
- Fixed sending of KPI events after feature toggling

## [2.4.0] - 2017-12-18

### Added
- Nakadi collects access log events
- Sending of KPI metrics for event-types count

## [2.3.4] - 2017-12-14

### Changed
- Optimized stream initialization

## [2.3.3] - 2017-12-12

### Added
- Periodically recreate topology listener while streaming subscription

### Fixed
- Fix concurrency issue when switching Timelines
- NullPointer when changing additionalProperties from true to Object.

## [2.3.2] - 2017-12-05

### Fixed
- Fixed issue with subscription stats, which would fail when the last committed offset is on a storage that was deleted
- Fixed issue with deletion of event types which had obsolete timelines

## [2.3.0] - 2017-11-14

### Added
- Switch from local token info service to remote one
- Change default storage via env var or at runtime 

### Changed
- Limited stream_timeout for consumption to 1h ± 10min

## [2.2.9] - 2017-11-14

### Fixed
- Fixed displaying of streamId for /stats endpoint

## [2.2.8] - 2017-11-01

### Fixed
- Massive topic deletion while switching timelines is now made with small interval between deletions

## [2.2.7] - 2017-10-25

### Changed
- Improve SLO log with application and event type names

## [2.2.6] - 2017-10-17

### Changed
- Create first timeline for event type by default

## [2.2.4] - 2017-10-13

### Fixed
- Optimized reactions on offset change while streaming subscription
- Committing with empty X-Nakadi-StreamId causes 503

### Added
- Create event type with SAFE rack-awareness

## [2.2.2] - 2017-09-28

### Added
- New database table for authorization
- Endpoints for getting and updating the lists of administrators
- Default administrator defined in application.yml
- Allow admins to bypass event type authorization restrictions

### Changed
- Admin endpoints use list of admins from Postgres


## [2.2.1] - 2017-09-26

### Changed
- Added logging of POST requests with compressed body.

## [2.2.0] - 2017-08-29

### Added
- Enable lz4 compression type for Kafka producer

## [2.1.2] - 2017-08-24

### Fixed
- Fixed DEBUG-level logging

## [2.1.1] - 2017-08-22

### Fixed
- Sync flush batches when using gzip streams.

## [2.1.0] - 2017-08-21

### Changed
- Using Jetty instead of Tomcat as Servlet container.

### Fixed
- Using Jetty fixes a rare concurrency issue where messages could be delivered to the wrong stream.

## [2.0.1] - 2017-08-11

### Fixed
- Added validation of offsets availability when resetting subscription cursors.

### Changed
- Removed authorization for subscription creation

## [2.0.0] - 2017-08-09

### Changed
- Changed imports format to have the same structure

### Removed
- Removed read_scopes and write_scopes from event types
- Removed CHECK_APPLICATION_LEVEL_PERMISSIONS feature

## [1.1.3] - 2017-08-03

### Fixed
- Fixed bug with incorrect lag calculation for subscirption.
- Optimized subscription stats endpoint for subscriptions with many event types inside.

### Changed
- Now it's possible to have a digit after the dot in event-type name.

## [1.1.2] - 2017-08-01

### Changed
- Updated kafka client library to 0.10.1.0

## [1.1.1] - 2017-07-26

### Fixed
- Returned back CHECK_APPLICATION_LEVEL_PERMISSIONS feature toggle.

## [1.1.0] - 2017-07-25

### Added
- The Nakadi manual merged to the project docs.
- The template added to generate Nakadi website using github pages.  
- Addition of a new authentication mode, 'REALM'

### Changed
- The metrics endpoint documentation key "summary" changed to "description" in Open API file.
- Event type authorization refactoring

### Removed
- Removed unused feature toggle CHECK_APPLICATION_LEVEL_PERMISSIONS
  for authorization based on owning_application.

### Fixed
- Fixed formatting of CursorDistanceResult in Open API file.

## [1.0.1] - 2017-07-14

### Fixed
- Fixed log level of InvalidStreamIdException.
- Fixed reading events with Low Level API from event type with expired timeline.

## [1.0.0] - 2017-07-12

### Added
- Authorization on reading from, modifying and publishing to an event type.
