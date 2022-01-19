---
title: Settings/Features
position: 20
---

## Settings/Features

Some features of the Nakadi can be enabled/disabled using the `settings/features` API.
Following are some of the important features you should be aware about.

### disable_event_type_creation
Sometimes we need to disable the creation of new event-types for operational reasons. 
The `disable_event_type_creation` feature allows you to temporarily disable creation of new event types.

### disable_event_type_deletion
Sometimes we need to disable the deletion of event-types for operational reasons. 
The `disable_event_type_deletion` feature allows you to temporarily disable deletion of event types.

### delete_event_type_with_subscriptions
The `delete_event_type_with_subscriptions` is helpful to allow deletion of event-types without an active subscription.
When this feature is enabled, even those event-types with subscriptions can be deleted.

### event_type_deletion_only_admins
Not in use anymore

### disable_subscription_creation
Sometimes we need to disable the creation of new subscriptions for operational reasons. 
The `disable_subscription_creation` feature allows you to temporarily disable creation of new subscriptions.

### remote_tokeninfo
Nakadi can be configured with two tokenInfo services; `local` and `remote`. 
By default Nakadi uses the `local` but Nakadi can be forced to use the `remote` one by enabling `remote_tokeninfo`.

### kpi_collection
Nakadi publishes several KPIs of Nakadi as special event-types in Nakadi. 
Publishing of these KPIs can be turned off by disabling `kpi_collection` if they are not used. 

### audit_log_collection
In addition to KPIs, Nakadi also publishes another set of events called audit logs. 
These can also be turned off by disabling `audit_log_collection`, if they are not used. 

### disable_db_write_operations
The `disable_db_write_operations` feature can be used to completely block all non-read access to the database. 
This can be useful when you want to do maintenance of the database.

### force_event_type_authz
Authorization section of an event-type can define who (or what) have which kind of access to an event-type.
When the `force_event_type_authz` feature is enabled, all new event-types must have an authorization section.

### force_subscription_authz
Same as event-type, subscription can also have an authorization section. 
The `force_subscription_authz` can be used to make sure that all new subscriptions have an authorization section.

### repartitioning
Nakadi supports repartitioning to increase the number of partitions available for an event-type.
This can be enabled by enabling the `repartitioning` feature.
