import os

class Settings(object):
    db_name = os.environ['MY_DB_NAME']
    db_hostname = os.environ['MY_DB_HOSTNAME']
    user = os.environ['MY_DB_USERNAME']
    password = os.environ['MY_DB_PASSWORD']
    token = os.environ['MY_POINTSBOT_TOKEN']
    bot_name = os.environ['MY_POINTSBOT_NAME']
    port = int(os.environ['MY_POINTSBOT_PORT'])
    bot_id = os.environ['MY_POINTSBOT_ID']
    bot_test_room_id = os.environ['MY_POINTSBOT_TEST_ROOM']
    admin_users = os.environ['MY_POINTSBOT_ADMIN_USERS'].split(",")
    metrics_bot_id = int(os.environ['METRICS_BOT_ID'])
    specific_limit = 30 #1 add/subtract command per <specific_limit> seconds
    general_limit = 5 #1 add/subtract command per <general_limit> seconds

class Titles(object):
    """
    Original Idea:
    0-99 - Private
    100 - 999 - Private 2nd Class
    1000 - 9999 - Private First Class
    10000 - 49999 - Specialist
    50000 - 99999 - Corporal
    100000 - 149999 - Sergeant
    150000 - 199999 - Staff Sergeant
    200000 - 249999 - Sergeant First Class
    """
    values = [0,100,500,1000,5000,10000,20000,30000,40000,50000,100000]
    titles = {
              100000:"Chairman of the Point",
              50000:"Vice Chairman of the Point",
              40000:"Chief Point Officer",
              30000:"Presipoint",
              20000:"Executive Vice Presipoint",
              10000:"Senior Vice Presipoint",
              5000:"Vice Presipoint",
              1000:"Pointager",
              500:"Assistant Pointager",
              100:"Full-Point Employee",
              0:"Pointern"
             }
