"""
Copyright 2016 Cisco Systems Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
import tornado.gen
import time
import base64, json, requests

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPRequest, HTTPError
from requests_toolbelt import MultipartEncoder

class Result(object):
    def __init__(self, result):
        self.headers = result.headers
        self.errors = None
        self.code = result.code
        try:
            self.body = json.loads(result.body.decode("utf-8"))
        except ValueError as e:
            self.errors = e

class Spark(object):

    def __init__(self, token, log=None):
        self.token = token
        self.log = log

    def printf(self, line):
        if self.log != None:
            log.info(line)
        else:
            print(line)

    def simple_request(self, url, data=None, method="GET"):
        headers={"Accept" : "application/json",
                "Content-Type":"application/json",
                "Authorization": "Bearer " + self.token}
        if data != None:
            if method in [None, "GET"]:
                method = "POST"
            return HTTPRequest(url, method=method, headers=headers, body=data, request_timeout=40)
        else:
            return HTTPRequest(url, method=method, headers=headers, request_timeout=40)

    @tornado.gen.coroutine
    def get(self, url):
        request = self.simple_request(url)
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(request)
        raise tornado.gen.Return(Result(response))

    def get_with_retries_std(self, url, results, index):
        retries = 0
        result = None
        while retries <= 2:
            try:
                request = self.simple_request(url)
                http_client = HTTPClient()
                response = http_client.fetch(request)
                result = Result(response)
                break
            except HTTPError as e:
                retries += 1
                msg = "HTTP Exception get_with_retries_std:{0}".format(e)
                self.printf(msg)
                self.printf(e.code)
                try:
                    try:
                        msg = json.loads(e.response.body.decode('utf8'))
                    except Exception as ex:
                        msg = e.response.body.decode('utf8')
                except Exception as exx:
                    pass#probably a 599 timeout
                if e.code in [429, 502, 599]:
                    if e.code == 429:
                        retry_after = None
                        try:
                            retry_after = e.response.headers.get("Retry-After")
                        except Exception as e:
                            pass
                        if retry_after == None:
                            retry_after = 30
                    else:
                        retry_after = 5
                    msg = "{0} hit, waiting for {1} seconds and then retrying...".format(e.code, retry_after)
                    self.printf(msg)
                    time.sleep(int(retry_after))
                else:
                    print("Not handling HTTPError:{0}.  url:{1}".format(e, url))
                    results[index] = [e, False]
                    return
        results[index] = [result, True]
        return

    @tornado.gen.coroutine
    def get_with_retries(self, url, websocket=None, max_retry_times=2):
        try:
            print("started get_with_retries")
            request = self.simple_request(url)
            http_client = AsyncHTTPClient()
            response = yield http_client.fetch(request)
            result = Result(response)
        except HTTPError as e:
            msg = "HTTP Exception get_with_retries:{0}".format(e)
            self.printf(msg)
            self.printf(e.code)
            try:
                try:
                    msg = json.loads(e.response.body.decode('utf8'))
                except Exception as ex:
                    msg = e.response.body.decode('utf8')
            except Exception as exx:
                pass#probably a 599 timeout
            if e.code in [429, 502, 599] and max_retry_times > 0:
                if e.code == 429:
                    retry_after = None
                    try:
                        retry_after = e.response.headers.get("Retry-After")
                    except Exception as e:
                        pass
                    if retry_after == None:
                        retry_after = 30
                else:
                    retry_after = 5
                msg = "{0} hit, waiting for {1} seconds and then retrying...".format(e.code, retry_after)
                self.printf(msg)
                if websocket != None:
                    update_obj = {"resource":"http_error", "message":msg, "update":True}
                    websocket.write_message(json.dumps(update_obj))
                yield tornado.gen.sleep(int(retry_after))
                max_retry_times-=1
                result = yield self.get_with_retries(url, websocket, max_retry_times)
            else:
                raise tornado.gen.Return(e)
        print("finished get_with_retries")
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def get_me(self):
        url = 'https://api.ciscospark.com/v1/people/me'
        response = yield self.get(url)
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def put(self, url, data):
        request = self.simple_request(url, json.dumps(data), method="PUT")
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(request)
        raise tornado.gen.Return(Result(response))

    @tornado.gen.coroutine
    def delete(self, url):
        request = self.simple_request(url, method="DELETE")
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(request)
        raise tornado.gen.Return(Result(response))

    @tornado.gen.coroutine
    def post(self, url, data):
        request = self.simple_request(url, json.dumps(data))
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(request)
        raise tornado.gen.Return(Result(response))

    def upload(self, roomId, name, path, filetype, markdown='', personId=None):
        try:
            url = "https://api.ciscospark.com/v1/messages"
            my_fields={'markdown': markdown,
                       'files': (name, open(path, 'rb'), filetype)
                       }
            if roomId == None:
                my_fields.update({'toPersonId': personId})
            else:
                my_fields.update({'roomId': roomId})
            print(my_fields)
            m = MultipartEncoder(fields=my_fields)
            attempts = 0
            while attempts < 3:
                try:
                    r = requests.post(url, data=m,
                                      headers={'Content-Type': m.content_type,
                                               'Authorization': 'Bearer ' + self.token})
                    print("Code:{}".format(r.status_code))
                    if r.status_code == 200:
                        break
                    attempts += 1
                except Exception as ex:
                    print("UPLOAD file send error:{}".format(ex))
                    attempts += 1
            try:
                jmsg = r.json()
                if jmsg.get("errorCode") != None:
                    print("Error - Tracking ID: {0}".format(r.headers.get("Trackingid")))
                    jmsg.update({"message": "{0} {1}".format(r.status_code, jmsg.get("message"))})
                print(jmsg)
            except Exception as ex:
                print("Exception:{}".format(ex))
                print("Exception - Tracking ID: {0}".format(r.headers.get("Trackingid")))
                jmsg = {"errorCode":r.status_code, "message":"{0} {1}".format(r.status_code, r.reason)}
                print(jmsg)
        except Exception as e:
            print("MAIN UPLOAD file error:{}".format(e))

    def upload_queue(self, roomId, name, path, filetype, markdown, queue):
        try:
            url = "https://api.ciscospark.com/v1/messages"
            my_fields={'roomId': roomId,
                       'markdown': markdown,
                       'files': (name, open(path, 'rb'), filetype)
                       }
            m = MultipartEncoder(fields=my_fields)
            r = requests.post(url, data=m,
                              headers={'Content-Type': m.content_type,
                                       'Authorization': 'Bearer ' + self.token})
            self.printf("Code: {0}".format(r.status_code))
            try:
                jmsg = r.json()
                if jmsg.get("errorCode") != None:
                    self.printf("Error - Tracking ID: {0}".format(r.headers.get("Trackingid")))
                    jmsg.update({"message": "{0} {1}".format(r.status_code, jmsg.get("message"))})
                queue.put(jmsg)
            except Exception as ex:
                self.printf("Exception {0}".format(ex))
                self.printf("Exception - Tracking ID: {0}".format(r.headers.get("Trackingid")))
                jmsg = {"errorCode":r.status_code, "message":"{0} {1}".format(r.status_code, r.reason)}
                queue.put(jmsg)
        except Exception as e:
            self.printf("UPLOAD file error {0}".format(e))
            queue.put(e)
