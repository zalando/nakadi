import os

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_CLIENTS_MAX_POOL_SIZE = int(os.environ.get('KAFKA_CLIENTS_MAX_POOL_SIZE', '16'))
KAFKA_CLIENTS_INIT_POOL_SIZE = int(os.environ.get('KAFKA_CLIENTS_INIT_POOL_SIZE', '4'))
ARUHA_LISTEN_PORT = int(os.environ.get('ARUHA_LISTEN_PORT', '8080'))

# OAuth2 settings
TOKEN_INFO_URL = os.environ.get('HTTP_TOKENINFO_URL', 'https://auth.zalando.com/oauth2/tokeninfo')
UID_TO_POST_EVENT = os.environ.get('OAUTH2_UID_TO_POST_EVENT', 'stups_shop-updater-hack')
TOKEN_CACHE_SIZE = 1024
TOKEN_CACHE_TTL = 60
TOKEN_INFO_RETRY_LIMIT = 3
TOKEN_INFO_RETRY_WAIT_S = 3
TOKEN_INFO_TIMEOUT_S = 3