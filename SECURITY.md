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
# Contact

We acknowledge that every line of code that we write may potentially contain security issues.
We are trying to deal with it responsibly and provide patches as quickly as possible. 

We host our bug bounty program on HackerOne, it is currently private, therefore if you would like to report a vulnerability and get rewarded for it, please ask to join our program by filling this form:

https://corporate.zalando.com/en/services-and-contact#security-form

You can also send you report via this form if you do not want to join our bug bounty program and just want to report a vulnerability or security issue.
