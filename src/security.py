import logging
import requests
import time
from flask import request
from requests.exceptions import RequestException
from cachetools import ttl_cache

import config

class FailedTokenInfoEndpoint(Exception):
    pass


# Decorator that does OAuth2 authentication
def authenticate(function):

    def function_wrapper(*args, **kwargs):

        # get oauth2 token from "Authorization" header; expected format: "Bearer ACTUAL_TOKEN"
        authorization_header = request.headers.get('Authorization')
        if authorization_header is None or not authorization_header.startswith('Bearer '):
            logging.info('[#OUATH_401] Authorization header problem')
            return {'detail': 'Not Authorized; Authorization header is missing or not well-formed.'}, 401

        #get pure token without "Bearer "
        token = authorization_header[len('Bearer: ') - 1:]

        # get token info
        try:
            response = get_token_info(token)
        except FailedTokenInfoEndpoint:
            return {'detail': 'Authentication check temporary not available'}, 503

        # check if the token is valid
        if response is None or response.status_code != requests.codes.ok:
            if response is not None:
                logging.info('[#OUATH_401] Invalid token. Response: %s', response.json())
            else:
                logging.info('[#OUATH_401] Response is None') # should not happen actually
            return {'detail': 'Not Authorized. Authentication token is invalid or expired.'}, 401

        # set token info so that it can be used later
        request.token_info = response.json()

        # call the function itself
        return function(*args, **kwargs)

    return function_wrapper


# Function that gets token info. Also does caching and retrying
@ttl_cache(config.TOKEN_CACHE_SIZE, config.TOKEN_CACHE_TTL)
def get_token_info(token):

    retry_attempts = 0
    while retry_attempts < config.TOKEN_INFO_RETRY_LIMIT:

        try:
            response = requests.get(config.TOKEN_INFO_URL, params={'access_token': token}, timeout=config.TOKEN_INFO_TIMEOUT_S)
            return response

        except RequestException as e:
            logging.info('[#OUATH_RETRY] Error while calling token_info endpoint: %s', e)
            retry_attempts += 1
            time.sleep(config.TOKEN_INFO_RETRY_WAIT_S)

    logging.error('[#OUATH_FAIL] Failed to do OAUTH check after %s retries', retry_attempts)
    raise FailedTokenInfoEndpoint()