import tornado.gen
import tornado.web

class AliveHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        self.write('Alive/Ready')
        self.finish()