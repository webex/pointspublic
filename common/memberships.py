import json

import tornado.gen
import tornado.web


class MembershipsHandler(tornado.web.RequestHandler):
    def printf(self, line):
        log = self.application.settings.get('log')
        if log != None:
            log.info(line)
        else:
            print(line)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        retVal = "true"
        try:
            req = self.request.body
            data = json.loads(req)
            self.printf(data)
            msg = None
            if data['data']['personId'] == self.application.settings['settings'].bot_id:
                if data['data']['isModerator'] == True:
                    try:
                        membership_id = data['data']['id']
                        result = yield self.application.settings['spark'].put("https://api.ciscospark.com/v1/memberships/{0}".format(membership_id),
                                                                          {"isModerator":False})
                        msg = "I automatically removed myself from being a space moderator."
                    except Exception as e:
                        self.printf("UpdateMembership Exception: {0}".format(e))
                        msg = "I failed to remove myself as a moderator. "
                        try:
                            msg += "{0} {1}\n\n".format(e.code, e.message)
                            self.printf("Tracking ID: {0}".format(e.response.headers['Trackingid']))
                        except Exception as ex:
                            self.printf("Code or Body exception: {0}".format(ex))
                        self.printf(msg)
            if msg != None:
                result = yield self.application.settings['spark'].post("https://api.ciscospark.com/v1/messages",
                                                                       {"toPersonId": data['actorId'], "markdown": msg})
        except Exception as exx:
            self.printf("Memberships General Exception: {0}".format(exx))
            retVal = "false"
        self.write(retVal)
