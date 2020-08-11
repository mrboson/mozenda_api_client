from redis import StrictRedisCluster

MOZENDA_BASE_URL = 'https://api.mozenda.com'
MOZENDA_API_KEY = 'YourAPIKeyGoesHere'
MOZENDA_PRIORITY_API_KEY = 'YourPriorityAPIKeyGoesHere'
MOZENDA_QUERY_TIMEFRAME = 65
MOZENDA_MAX_PAGES = 2

ENVIRONMENT = 'dev'

REDIS_CLUSTER_NODES = [
    {"host": "redis-node-1", "port": "6379"},
    {"host": "redis-node-2", "port": "6379"},
    {"host": "redis-node-3", "port": "6379"},
]

REDIS = {
    'default': StrictRedisCluster(startup_nodes=REDIS_CLUSTER_NODES, decode_responses=False),
}
