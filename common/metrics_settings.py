import os

class MetricsSettings(object):
    db_name = "bot_metrics"
    metrics_table = "metrics"
    domains_table = "domains"
    bots_table = "bots"
    db_hostname = os.environ.get('MY_METRICS_DB_HOSTNAME', os.environ.get('MY_DB_HOSTNAME'))
    user = os.environ.get('MY_METRICS_DB_USERNAME', os.environ.get('MY_DB_USERNAME'))
    password = os.environ.get('MY_METRICS_DB_PASSWORD', os.environ.get('MY_DB_PASSWORD'))
    ca_path = os.environ.get('MY_METRICS_DB_CA_PATH', os.environ.get('MY_DB_CA_PATH'))
    metrics_bot_id = int(os.environ['METRICS_BOT_ID'])
