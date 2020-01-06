# -*- coding: utf-8 -*-
#!/usr/bin/env python
import sys;
reload(sys);
sys.setdefaultencoding("utf8")

import json
import os
import re
import time
import traceback

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web

from pointsbot.src.rds_mysqldb import PointsDB
from pointsbot.src.settings import Settings, Titles
from common.alive import AliveHandler
from common.memberships import MembershipsHandler
from common.spark_py2 import Spark
from common.rds_mysql_metrics import MetricsDB

from tornado.options import define, options, parse_command_line
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError

define("port", default=Settings.port, help="run on the given port", type=int)
define("debug", default=False, help="run in debug mode")

class Commands(object):
    leaderboard = ["leaderboard", "scores", "highscores", "highscoring", "highest", "topscores", "top"]
    check = ["check", "view", "inspect", "howmany", "points", "get", "status"]
    add = ['to', 'give', 'add', 'award', 'increment', 'grant', 'bestow', 'reward', 'plus', '++', '+']
    subtract = ['from', 'take', 'subtract', 'remove', 'decrement', 'destroy', 'eliminate', 'annihilate', 'minus', '--', '-']
    points_dict = {"one":1, "two":2, "three":3, "four":4, "five":5, "six": 6, "seven":7, "eight":8, "nine":9, "ten":10}


def help_msg():
    msg = "Only 1 point can be added or subtracted at a time.  Examples:\n\n"
    msg += "**Points!** to **John**  \n"
    msg += "**Points!** johnsmi@example.com++  \n"
    msg += "**Points!** **John**++  \n"
    msg += "**Points!** from johnsmi@example.com  \n"
    msg += "**Points!** **John**\-\-  \n"
    msg += "**Points!** check **John**  \n"
    msg += "**Points!** check me  \n"
    msg += "**Points!** check team me/**John**  \n"
    msg += "**Points!** check org me/**John**  \n"
    msg += "**Points!** titles  \n"
    msg += "**Points!** leaderboard  \n"
    msg += "**Points!** leaderboard team  \n"
    msg += "**Points!** leaderboard org  \n"
    msg += "* leaderboard/check will return only the points accrued in this space.  \n"
    msg += "* leaderboard team/check team will return only the points accrued in the team in which this space belongs.  \n"
    msg += "* leaderboard org/check org will return the cumulative score across all spaces for user(s) belonging to the org of the requesting user.\n\n"

    msg += "In the above, **John** is a mentioned user and johnsmi@example.com is the email address to the same user's Webex Teams account.  "
    msg += "Points can also be given to any string, like '**Points!** to batman' or '**Points!** to john', "
    msg += "but that usage will not be linked to a Webex Teams account or mentioned user.\n\n"
    msg += "You can also award points to multiple people or things at once.  Examples:  \n"
    msg += "**Points!** to **Jane**, **John**, batman, john  \n"
    msg += "**Points!** to **Jane** **John** batman john  \n"
    return msg


class MainHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def leaderboard(self, in_message, msg, room_id, requester_person_id):
        prior, sort_type = in_message.split("leaderboard")
        sort_type = sort_type.strip()
        scores = None
        if sort_type == "team":
            print "Team Sort"
            spark_room = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/rooms/{0}'.format(room_id))
            team_id = spark_room.body.get('teamId')
            if team_id == None:
                msg = "This space does not belong to a team."
            else:
                scores = self.application.settings['db'].leaderboard("teamId", team_id)
                msg = "The leaderboard for this team, across all team spaces:  \n"
        elif sort_type in ["org", "organization"]:
            sort_type == "org"
            print "Org Sort"
            spark_person = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/people/{0}'.format(requester_person_id))
            org_id = spark_person.body.get('orgId')
            scores = self.application.settings['db'].leaderboard("orgId", org_id)
            msg = "The leaderboard for this org, across all spaces:  \n"
        else:
            print "sort_type: {0}, defaulting to room sort.".format(sort_type)
            sort_type = "space"
            scores = self.application.settings['db'].leaderboard("roomId", room_id)
            msg = "The leaderboard for this space:  \n"
        print "SCORES", scores
        if scores not in [None, [], ()]:
            for index in range(len(scores)):
                msg += "{0}. ".format(index+1) + self.score_msg(scores[index][0], scores[index][1])
        elif scores in [[], ()]:
            whose = "this"
            if sort_type == "org":
                whose = "your"
            msg = "No scores for {0} {1} yet.  Try giving someone points!\n\n".format(whose, sort_type)
        raise tornado.gen.Return(msg)

    @tornado.gen.coroutine
    def check(self, after_mention, msg, room_id, requester_person_id):
        print "check command!"
        try:
            command, value_str = after_mention.strip(" ").replace("</p>","").split(" ", 1)
            check_type = "roomId"
            check_value = room_id
            if value_str.startswith('team'):
                value_str = value_str.replace('team','').strip(" ")
                spark_room = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/rooms/{0}'.format(room_id))
                check_type = "teamId"
                check_value = spark_room.body.get('teamId')
                if check_value == None:
                    msg = "This space does not belong to a team.  \n"
            elif value_str.startswith('org'):
                value_str = value_str.replace('org','').strip(" ")
                spark_person = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/people/{0}'.format(requester_person_id))
                check_type = "orgId"
                check_value = spark_person.body.get('orgId')
            if check_value != None:
                while len(value_str) > 0:
                    print "value_str:", value_str
                    person = None
                    display_name = None
                    if value_str.startswith('<spark-mention'):
                        #If the person was mentioned:
                        person, value_str = value_str.split('</spark-mention>',1)
                        before, person = person.split('data-object-id="',1)
                        person, remaining = person.split('">',1)
                        display_name = remaining
                        print "remaining", remaining
                        row = self.application.settings['db'].get(check_type, check_value, "personId", person)
                    else:
                        try:
                            person, value_str = value_str.split(' ',1)
                        except ValueError as ve:
                            person = value_str.strip(" ")
                            value_str = ""
                        if "@" in person:
                            row = self.application.settings['db'].get(check_type, check_value, "email", person)
                        elif person == "me":
                            print "me?", person
                            display_name = "You"
                            person = requester_person_id
                            row = self.application.settings['db'].get(check_type, check_value, "personId", person)
                        else:
                            row = self.application.settings['db'].get(check_type, check_value, "name", person)
                    value_str = value_str.strip(" ")
                    print "row:", row
                    if row is not None and row[PointsDB.NAME] != None:
                        if display_name == None:
                            display_name = row[PointsDB.DISPLAY_NAME]
                        msg += self.score_msg(display_name, row[PointsDB.POINTS], check_type)
                    else:
                        if display_name is not None:
                            msg += self.score_msg(display_name, 0, check_type)
                        else:
                            if person in ['org', 'team']:
                                msg += "**{0}** should immediately follow the **check** command. For example: check {0} me  \n".format(person)
                            else:
                                msg += self.score_msg(person, 0, check_type)
        except ValueError as vex:
            msg += "Please provide a value to check for points.  \n"
        print "msg:", msg
        raise tornado.gen.Return(msg)

    @tornado.gen.coroutine
    def update_points(self, msg, room_id, person, is_id, points, requester_person_id, requester_person_email):
        invalid_person = False
        updated_item = None
        print "Person:", person,
        if is_id:
            key = "personId"
        elif "@" in person:
            key = "email"
        else:
            key = "name"
        if person in ["me", requester_person_id, requester_person_email]:
            msg += "You cannot give yourself points.  \n"
        elif person in ["org", "team"]:
            msg += "{0} is a reserved word for this bot.  \n".format(person)
        else:
            result = self.application.settings['db'].get("roomId", room_id, key, person)
            if result == None or result[PointsDB.NAME] == None:
                print "Item does not exist! Inserting..."
                spark_room = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/rooms/{0}'.format(room_id))
                team_id = spark_room.body.get('teamId')
                if is_id:
                    spark_person = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/people/{0}'.format(person))
                    display_name = spark_person.body.get('displayName')
                    email = spark_person.body.get('emails', [None])[0]
                    org_id = spark_person.body.get('orgId')
                    self.application.settings['db'].insert(room_id, display_name, points, email, person, display_name, org_id, team_id)
                elif "@" in person:
                    spark_person = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/people?email={0}'.format(person))
                    items = spark_person.body.get('items')
                    try:
                        person_id = items[0].get('id')
                        display_name = items[0].get('displayName')
                        org_id = items[0].get('orgId')
                        self.application.settings['db'].insert(room_id, display_name, points, person, person_id, display_name, org_id, team_id)
                    except IndexError as ie:
                        invalid_person = True
                        msg += "{0} is not a valid Webex Teams account email.  \n".format(person)
                else:
                    self.application.settings['db'].insert(room_id, person, points, teamId=team_id)
            else:
                print "Need to update!"
                if result[PointsDB.NAME] in self.application.settings['givers_specific'].get(requester_person_id,{}):
                    invalid_person = True
                    msg += "You cannot give/take points to/from the same user more than once every {0} seconds.  \n".format(Settings.specific_limit)
                else:
                    self.application.settings['db'].update(room_id, key, person, points)
            if not invalid_person:
                row = self.application.settings['db'].get("roomId", room_id, key, person)
                updated_item = row[PointsDB.NAME]
                print "row:", row
                s = ""
                if row[PointsDB.POINTS] != 1:
                    s = "s"
                msg += "**{0}** now has {1} point{2} in this room!  \n".format(row[PointsDB.DISPLAY_NAME], row[PointsDB.POINTS], s)
                if row[PointsDB.POINTS] == 5000 and points > 0:
                    user = updated_item
                    if row[PointsDB.EMAIL] != None:
                        user = row[PointsDB.EMAIL]
                    markdown = "**{0}** has reached 5000 points - orgId: {1}".format(user, row[PointsDB.ORG_ID])
                    print markdown
                    for user in Settings.admin_users:
                        yield self.application.settings['spark'].post("https://api.ciscospark.com/v1/messages",
                                                       {"toPersonEmail": user,
                                                        "markdown": markdown})
                    for msg_room_id in [Settings.bot_test_room_id]:
                        yield self.application.settings['spark'].post("https://api.ciscospark.com/v1/messages",
                                                       {"roomId": msg_room_id,
                                                        "markdown": markdown})
        raise tornado.gen.Return((msg, updated_item))

    def score_msg(self, user, points, score_type=None):
        s = ""
        if points != 1 and points != -1:
            s = "s"
        rank_value = 0
        for value in Titles.values:
            if points < value:
                break
            rank_value = value
        rank = Titles.titles[rank_value]
        if score_type == None:
            clarifier = ""
        elif score_type == "roomId":
            clarifier = "in this room "
        elif score_type == "teamId":
            clarifier = "on this team "
        elif score_type == "orgId":
            clarifier = "in your org "
        if user.lower() == "you":
            prefix = "**You** have "
        else:
            prefix = "**{0}** has ".format(user)
        return prefix + "{0} point{1} {2}- *{3}*  \n".format(points, s, clarifier, rank)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        """
        """
        print("BODY:{0}".format(self.request.body))
        webhook = json.loads(self.request.body)
        m_command = None
        personEmail = None
        in_message = None
        if webhook['data']['personId'] != Settings.bot_id:
            personEmail = webhook['data'].get('personEmail')
            result = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/messages/{0}'.format(webhook['data']['id']))
            in_message = result.body.get('text', '').lower()
            in_message = in_message.replace(Settings.bot_name, "", 1).strip()
            room_id = webhook['data']['roomId']
            room_details = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/rooms/{0}'.format(room_id))
            msg = ""
            if room_details.body.get("type") == "direct":
                msg += "This bot can only be used in group spaces."
            else:
                requester_person_id = webhook['data']['personId']
                person_details = yield self.application.settings['spark'].get('https://api.ciscospark.com/v1/people/{0}'.format(requester_person_id))
                if person_details.body.get("type") == "bot":
                    msg += "This bot does not allow messages from other bots."
                else:
                    m_command = in_message
                    if in_message == "help":
                        msg = help_msg()
                    elif in_message == "titles":
                        msg += "**Minimum Points** - *Title*  \n"
                        for value in reversed(Titles.values):
                            msg += "**{0}** - *{1}*  \n".format(value, Titles.titles[value])
                    elif any(in_message.startswith(x) for x in Commands.leaderboard):
                        m_command = "leaderboard"
                        msg = yield self.leaderboard(in_message, msg, room_id, requester_person_id)
                    else:
                        split_on = '<spark-mention data-object-type="person" data-object-id="{0}">'.format(Settings.bot_id)
                        before_mention, after_mention = result.body.get('html', '').rsplit(split_on,1)
                        before_mention, after_mention = after_mention.split('</spark-mention>',1)
                        if any(in_message.startswith(x) for x in Commands.check):
                            msg = yield self.check(after_mention, msg, room_id, requester_person_id)
                        else:
                            m_command = "points_unknown"
                            print "Givers general:", self.application.settings['givers_general']
                            print "Givers specific:", self.application.settings['givers_specific']
                            now = time.time()
                            for giver in dict(self.application.settings['givers_general']):
                                if self.application.settings['givers_general'][giver] < now - Settings.general_limit:
                                    self.application.settings['givers_general'].pop(giver)
                            for giver in dict(self.application.settings['givers_specific']):
                                for awardee in dict(self.application.settings['givers_specific'][giver]):
                                    if self.application.settings['givers_specific'][giver][awardee] < now - Settings.specific_limit:
                                        self.application.settings['givers_specific'][giver].pop(awardee)
                                if self.application.settings['givers_specific'][giver] == {}:
                                    self.application.settings['givers_specific'].pop(giver)
                            if requester_person_id in self.application.settings['givers_general']:
                                msg = "You cannot give or take points more than once every {0} seconds.  \n".format(Settings.general_limit)
                            else:
                                if after_mention.strip(" ").startswith('<spark-mention'):
                                    command = "add"
                                    value_str = after_mention.strip(" ")
                                else:
                                    try:
                                        command, value_str = after_mention.strip(" ").split(" ", 1)
                                        command = command.replace(",","")
                                        if (command.endswith("++") or command.endswith('--') or command.endswith(u"—")
                                            or value_str.startswith('++') or value_str.startswith('--') or value_str.startswith(u"—")):
                                            value_str = after_mention.strip(" ")
                                            command = "add"
                                    except ValueError as ve:
                                        command = "add"
                                        value_str = after_mention.strip(" ")
                                value_str = value_str.replace(","," ").replace("</p>","")
                                print "command", command.encode('utf-8')
                                if command not in Commands.add and command not in Commands.subtract:
                                    print "**{0}** is not a valid command.  \n".format(command)
                                    value_str = command + " " + value_str #adding the command back in this case
                                    command = "add"
                                m_command = command
                                while len(value_str) > 0:
                                    print "value_str:", value_str.encode('utf-8')
                                    is_id = False
                                    points = None
                                    if value_str.startswith('<spark-mention'):
                                        #If the person was mentioned:
                                        person_id, value_str = value_str.split('</spark-mention>',1)
                                        before, person_id = person_id.split('data-object-id="',1)
                                        person_id, remaining = person_id.split('">',1)
                                        person = person_id
                                        is_id = True
                                    else:
                                        #The person was not mentioned
                                        try:
                                            person, value_str = value_str.split(' ',1)
                                        except ValueError as ve:
                                            person = value_str.strip(" ")
                                            value_str = ""
                                    value_str = value_str.strip(" ")
                                    if person not in ['to', 'from', 'and', '', ' ', '+', '-', '++', '--', u"—"]:
                                        #++, -- etc will override the command given at the start
                                        if person.endswith('--') or person.endswith(u"—"):
                                            person = person.replace("--","").replace(u"—","")
                                            points = -1
                                        elif person.endswith('++'):
                                            person = person.replace("++","")
                                            points = 1
                                        else:
                                            #This is different from the above two conditions
                                            #value_str will start with these values (++, --) if following a mention
                                            if value_str.startswith('++'):
                                                value_str = value_str[2:].strip(" ")
                                                points = 1
                                            elif value_str.startswith('--'):
                                                value_str = value_str[2:].strip(" ")
                                                points = -1
                                            elif value_str.startswith(u"—") or value_str.startswith("-"):
                                                value_str = value_str[1:].strip(" ")
                                                points = -1
                                            elif value_str.startswith("+"):
                                                value_str = value_str[1:].strip(" ")
                                                points = 1
                                            else:
                                                #if no ++ or -- values, then use the command itself
                                                if command in Commands.add:
                                                    points = 1
                                                elif command in Commands.subtract:
                                                    points = -1
                                        next = value_str.split(" ")[0]
                                        try:
                                            int_next = int(next)
                                            msg += "**{0}** detected. Numeric values are ignored. Only 1 point can be added or subtracted at a time.  \n".format(next)
                                            value_str = value_str.replace(next, "", 1)
                                            if int_next >= 0:
                                                points = 1
                                            else:
                                                points = -1
                                        except ValueError as ve:
                                            #We actually want this exception to occur
                                            pass
                                        try:
                                            int(person)
                                            msg += "Numbers are not allowed.  **{0}** detected.  \n".format(person)
                                            points = None
                                        except ValueError as ve:
                                            #We actually want this exception to occur
                                            pass
                                        if points != None:
                                            if points == 1:
                                                m_command = "add"
                                            elif points == -1:
                                                m_command = "subtract"
                                            msg, updated_item = yield self.update_points(msg, room_id, person, is_id, points, requester_person_id, webhook['data']['personEmail'])
                                            print "updated_item:", updated_item
                                            self.application.settings['givers_general'].update({requester_person_id:time.time()})
                                            if updated_item != None:
                                                if self.application.settings['givers_specific'].has_key(requester_person_id):
                                                    self.application.settings['givers_specific'][requester_person_id].update({updated_item:time.time()})
                                                else:
                                                    self.application.settings['givers_specific'].update({requester_person_id:{updated_item:time.time()}})
                                if msg == "":
                                    #msg should not still be empty at this point
                                    msg = "'{0}' is not a valid command or input.".format(after_mention.replace("</p>","").strip(" "))
            if m_command != None and in_message != None and personEmail != None:
                self.application.settings['metrics_db'].insert(personEmail, m_command, in_message)
            if msg != "":
                print "msg:", msg
                print room_id
                yield self.application.settings['spark'].post('https://api.ciscospark.com/v1/messages', {'markdown':msg, 'roomId':room_id})
        self.write("true")


@tornado.gen.coroutine
def main():
    try:
        parse_command_line()
        app = tornado.web.Application(
            [
                (r"/", MainHandler),
                (r"/alive", AliveHandler),
                (r"/ready", AliveHandler),
                (r"/memberships", MembershipsHandler),
                ],
            #template_path=os.path.join(os.path.dirname(__file__), "templates"),
            #static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=False,
            debug=options.debug,
            )
        app.settings['settings'] = Settings
        app.settings['db'] = PointsDB()
        app.settings['metrics_db'] = MetricsDB()
        app.settings['spark'] = Spark(Settings.token)
        app.settings['givers_general'] = {}
        app.settings['givers_specific'] = {}
        server = tornado.httpserver.HTTPServer(app)
        server.bind(options.port)  # port
        print("Serving... on port {0}".format(Settings.port))
        server.start()
        tornado.ioloop.IOLoop.instance().start()
        print('Done')
    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
