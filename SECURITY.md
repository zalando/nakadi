# Security Model


*Nakadi* makes use of Oauth2 to provide security, there are three modes that can be 
configured. In order to configure security for *Nakadi* you need to edit the *Nakadi* application.yml (env vars)
 and add oauth2 validation endpoints. This is set via two environmental variables.
 
 * AuthenticationMode: Security model to use 
 
 * TokenInfoUrl: oauth2 endpoint to get token information

## Open

* AuthenticationMode: off


Allows open access to *Nakadi*. This mode only exists for testing purposes.

## Token Validation

* AuthenticationMode: basic

Validates the Token, and only the token

## Token and Scope Validation

* AuthenticationMode: full

Validates the scopes of the token as well as the existence of the token. 

***
# Contacts 
<team-aruhah@zalando.de>
