from __future__ import print_function
from flask import Flask, make_response, request, Response, json
from flask_restful import Resource, Api, reqparse
import requests
import os,sys,ast
import copy
from datetime import datetime, date, time

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()

key_value_db = {}
buffered_keys = {}

view_list = os.environ.get('VIEW').split(',')
my_ip = os.environ.get('IP_PORT')

def max(a,b):
    if a > b:
        return a
    return b

def storeKeyValue(ipPort, key, value, payload):
    return requests.put( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'val':value, 'payload': json.dumps(payload)})

def deleteView(ipPort, view):
    del local_vc.clock[ipPort]
    return requests.delete( 'http://%s/view'%(str(ipPort)), data={'ip_port':view})

def addView(ipPort, view):
    local_vc.clock[ipPort] = 0
    return requests.put( 'http://%s/view'%(str(ipPort)), data={'ip_port':view})
    # return requests.put( 'http://%s/view'%str(ipPort), data={'ip_port':newAddress} )

def deleteB(ipPort, key, payload):
    return requests.delete( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': json.dumps(payload)})

def broadcast(key, value, payload):
    for ipPort in view_list:
        if ipPort != my_ip:
            storeKeyValue(ipPort, key, value, payload)

def broadcastViewDelete(view):
    for ipPort in view_list:
        if ipPort != my_ip:
            deleteView(ipPort, view)

def broadcastViewAdd(view):
    for ipPort in view_list:
        if ipPort != my_ip:
            addView(ipPort, view)

def broadcastDelete(key, payload):
    for ipPort in view_list:
        if ipPort != my_ip:
            deleteB(ipPort, key, payload)


class vector_clock():
    def __init__(self):
        self.clock = {}
        self.timestamp = datetime.now()
        # self.IP = os.environ.get('IP_PORT')
        for node in view_list:
            self.clock[node] = 0

    def increment_clock(self):
        self.clock[my_ip] += 1

    def update_clock(self, other):
        # self.clock[other.IP] = other.clock[other.IP]
        for ip in self.clock:
            if other.clock[ip] > self.clock[ip]:
                self.clock[ip] = other.clock[ip]

    def __lt__(self,other):
        all_equal = True
        for ip in self.clock:
            if self.clock.get(ip) > other.clock.get(ip):
                return False
            elif self.clock.get(ip) < other.clock.get(ip):
                all_equal = False
        return not all_equal

    def __gt__(self,other):
        all_equal = True
        for ip in self.clock:
            if self.clock.get(ip) < other.clock.get(ip):
                return False
            elif self.clock.get(ip) > other.clock.get(ip):
                all_equal = False
        return not all_equal

    def __eq__(self,other):
        for ip in self.clock:
            if self.clock.get(ip) != other.clock.get(ip):
                return False
        return True

    def to_dict(self):
	    return self.__dict__

    # def copy(self, other):
    #     self.clock = other.clock

class kvs_node(Resource):
    def handle_put(self, key, data, ts):

        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg': 'Key not valid',
                'result': 'Error'
            }), status=400, mimetype=u'application/json')

        # if len(value) > 1000000:
        #     return Response(json.dumps({
        #         'result': 'Error',
        #         'msg': 'Object too large. Size limit is 1MB'
        #     }))
        # put_vc = vector_clock()
        # put_vc.clock = copy.deepcopy(local_vc.clock)
        # put_vc.increment_clock()
        if key not in key_value_db:
            key_value_db[key] = (data, ts)
            return Response(json.dumps({
                'replaced': False,
                'msg': 'Added successfully',
                'payload' : {},
            }), status=200, mimetype=u'application/json')

        else:
            key_value_db[key] = (data, ts)
            return Response(json.dumps({
                'replaced': True,
                'msg': 'Updated successfully',
                'payload' : {},
            }), status=201, mimetype=u'application/json')

    def get(self, key):
        print("Doing GET #1\n", file=sys.stderr)
        # local_vc.increment_clock()
        # Spec says nothing about key length
        # if len(key) > 200 or len(key) < 1:
        #     return Response(json.dumps({
        #         'msg':'Key not valid',
        #         'result' : 'Error'}))

        # Spec says to return key does not exist if not in db
        # along with payload and 404 status
        if key not in key_value_db:
            return Response(json.dumps({
                'result':'Error',
                'msg' : 'Key does not exist',
                'payload': {}
                }), status=404, mimetype=u'application/json')

        # Spec says to return value of key if key
        # in database along with payload with return status of 200
        return Response(json.dumps({
            'result' : 'Success',
            'value' : key_value_db[key][0],
            'payload' :  {},
        }), status=200, mimetype=u'application/json')

    def delete(self,key):
        hop_list = []
        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg':'Key not valid',
                'result' : 'Error'}))
        if key not in key_value_db:
            return Response(json.dumps({
                'result':'Error',
                'msg':'Key does not exist',
                'payload':hop_list}),
                status=404, mimetype=u'application/json')
        del key_value_db[key]
        broadcastDelete(key, local_vc.to_dict())
        return Response(json.dumps({
            'result':'Success',
            'msg':'Key deleted',
            'payload' :  hop_list,
            }),
            status=200, mimetype=u'application/json')

    def put(self, key):
        # local_vc.increment_clock()
        value = request.form.get('val')
        payload = request.form.get('payload')

        print("\nPayload is type: %s\nPayload is: %s\n" %(type(payload),payload), file=sys.stderr)


        hop_list = list(ast.literal_eval(payload))

        print("Here's the hop set:",file=sys.stderr)
        print(hop_list,file=sys.stderr)
        if my_ip not in hop_list:
            hop_list.append(my_ip)
            print("Let's rebroadcast beyotch", file=sys.stderr)
            temp = self.handle_put(key, value, hop_list)
            broadcast(key, value, hop_list)
            return temp
        else:
            print("Im already in this bitch",file=sys.stderr)
        return

        # if value:
        #     msg_vc = vector_clock()
        #
        #     payload = ast.literal_eval(payload)
        #     msg_vc.clock = payload.get('clock')
        #
        #     print('local clock = %s' %local_vc.clock, file=sys.stderr)
        #     print('message clock = %s' %msg_vc.clock, file=sys.stderr)
        #     print('comparison = %s' %(local_vc < msg_vc), file=sys.stderr)
        #
        #     if local_vc < msg_vc:
        #         print("Let's rebroadcast beyotch", file=sys.stderr)
        #         print("Is the clock actually less than?", file=sys.stderr)
        #         local_vc.clock = copy.deepcopy(msg_vc.clock)
        #         temp = self.handle_put(key, value, msg_vc.timestamp)
        #         buffered_keys[key] = msg_vc.timestamp
        #         broadcast(key, value, local_vc.to_dict())
        #         return temp
        #     # case of concurrency
        #     if not local_vc < msg_vc and not msg_vc < local_vc:
        #         # equivalence
        #         if local_vc == msg_vc:
        #             # the node is up to date so the message is ignored
        #             # the value is removed from the buffered key
        #             del buffered_keys[key]
        #         else:
        #             # clocks are concurrent but not equal
        #
        #             # compare the key with the buffered keys
        #             # if the key is the same as message sent
        #             if key in buffered_keys:
        #                 # compare timestamps
        #                 if buffered_keys[key] < msg_key.timestamp:
        #                     # the messages timestamp is newer, so its value wins
        #                     temp = self.handle_put(key, value, ts)
        #                     broadcast(key, value, local_vc.to_dict())
        #                     return temp
        #                     # the messages value is older, so it is ignored
        #                     return
        #             else:
        #                 # if it is a different key, it can be put in
        #                 # update vector clock
        #                 local_vc.update_clock(msg_vc)
        #                 # broadcast
        #                 temp = self.handle_put(key, value, ts)
        #                 broadcast(key, value, local_vc.to_dict())
        #                 return temp
        #     return

class kvs_search(Resource):
  def get(self, key):
        print("Doing GET #2\n", file=sys.stderr)
        search_vc = vector_clock()
        print("New VC", file=sys.stderr)
        print(search_vc.clock, file=sys.stderr)
        search_vc.clock = copy.deepcopy(local_vc.clock)
        print("init VC", file=sys.stderr)
        print(search_vc.clock, file=sys.stderr)
        search_vc.increment_clock()
        print("incremented VC", file=sys.stderr)
        print(search_vc.clock, file=sys.stderr)
        print("Got to here after GET #2", file=sys.stderr)
        print(local_vc.clock, file=sys.stderr)

        hop_list = []

        if key not in key_value_db:
            return Response(json.dumps({
                'isExists': False,
                'result':'Success',
                'payload' :  hop_list}),
                status=200, mimetype=u'application/json')
        return Response(json.dumps({
            'isExists': True,
            'result':'Success',
            'payload':  hop_list}),
            status=200, mimetype=u'application/json')

class kvs_view(Resource):
    def get(self):
        print("Current [%s]'s view_list: %s" % (my_ip, view_list),file=sys.stderr)
        return Response(json.dumps({
            'view': ",".join(view_list),
        }),
        status=200, mimetype=u'application/json')

    def delete(self):
        #I hope all we need to check for is the port sent
        ip_port = request.form.get('ip_port')
        if ip_port not in view_list:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : request.form.get('ip_port') + " is not in current view"
            }),
            status=404, mimetype=u'application/json')
        else:
            view_list.remove(ip_port)
            broadcastViewDelete(ip_port)
            return Response(json.dumps({
                'result' : 'Success',
                'msg' : 'Successfully removed ' + request.form.get('ip_port') + ' from view'
            }),
            status=200, mimetype=u'application/json')

    def put(self):
        ip_port = request.form.get('ip_port')
        print("viewlist: %s" % view_list, file=sys.stderr)
        print("putting: %s" % ip_port, file=sys.stderr)
        if ip_port in view_list:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : request.form.get('ip_port') + ' is already in view'
            }),
            status=404, mimetype=u'application/json')
        else:
            # TODO: Actually add the node to the view and broadcast view change
            #I will attempt to implement the view change.
            view_list.append(ip_port)
            broadcastViewAdd(ip_port)
            for k,v in key_value_db.items():
                storeKeyValue(ip_port, k, v, local_vc.to_dict())
            print("newthing: %s" % view_list,file=sys.stderr)
            return Response(json.dumps({
                'result' : 'Success',
                'msg' : 'Successfully added ' + request.form.get('ip_port') + ' to view'
            }),
            status=200, mimetype=u'application/json')


api.add_resource(kvs_node, '/keyValue-store/<string:key>')
api.add_resource(kvs_search, '/keyValue-store/search/<string:key>')
api.add_resource(kvs_view, '/view')

print("ip and port = %s" %my_ip)

local_vc = vector_clock()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
