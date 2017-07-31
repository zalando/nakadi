# Change Log
All notable changes to `Nakadi` will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

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
