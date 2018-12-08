from __future__ import print_function
from flask import Flask, make_response, request, Response, json
from flask_restful import Resource, Api, reqparse
import requests
import os,sys,ast
import copy
from datetime import datetime#, date, time
import threading, random, time

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()

# KVS: spec
# value = (value: string, timestamp: datetime, vector_clock: dict, flag_deleted: boolean)
KVS_VAL_POS = 0
KVS_TS_POS = 1
KVS_VC_POS = 2
KVS_DEL_POS = 3

key_value_db = {}

view_list = os.environ.get('VIEW').split(',')
view_list.sort()
view = {'list': view_list, 'updated': time.time()}
view['list'].sort()
my_ip = os.environ.get('IP_PORT')

# initial number of shards
numShards = int(os.environ.get('S'))
# list of all shards in the system
shard_ids = []
# number of key-value pairs this shard is responsible for
numberOfKeys = 0
# this node's shard
shardID = 0
# list of all shard's members as IP addresses
shard_members = []

GOSSIP_DELAY = 0.3

# FUNCTION: dprint
# DESCRIPTION: print() for stderr so that it will actually work
# for Docker debugging


def dprint(msg):
    print(msg, file=sys.stderr)


def gossip_kvs():
    while True:
        time.sleep(GOSSIP_DELAY)
        peers = list(set(view['list']) - {my_ip})
        dprint("TRYNNA gossip")
        if len(peers) > 0:
            random_peer = peers[random.randint(0, len(peers) - 1)]
            dprint('gossipping kvs:%s'%key_value_db)
            for k in key_value_db:
                data, ts, vc, dflag = key_value_db[k]
                sendKey(random_peer, k, data, (ts, vc, dflag))


def gossip_view():
    time.sleep(6)
    while True:
        time.sleep(GOSSIP_DELAY)
        peers = list(set(view['list']) - {my_ip})
        if len(peers) > 0:
            random_peer = peers[random.randint(0, len(peers) - 1)]
            sendView(random_peer, view)


def shardNodes(shardSize):
    # if not enough nodes to shard into specified size, default to 1
    # NOTE: this is only for initialization, not manual view/shard changes
    global shard_members
    global shard_ids
    global shardID
    global numShards
    global view
    view['list'].sort()
    if ((len(view['list']))/2) < shardSize:
        shard_members = []
        numShards = 1
        shardID = 0
        shard_members.append(view['list'])
        shard_ids = ["0"]
    # if changing to one shard
    elif shardSize == 1:
        shard_members = []
        numShards = 1
        shardID = 0
        shard_members.append(view['list'])
        shard_ids = ["0"]
        # check if this node needs to migrate data
        if numberOfKeys != 0:
            reHashKeys()
    else:
        numShards = shardSize
        shard = []
        shard_members = [[] for i in range(shardSize)]
        for i in range(0, len(view['list'])):
            shard.append(i % shardSize)
        shardID = shard[view['list'].index(my_ip)]
        for i in range(0, shardSize):
            for j in range(0, len(view_list)):
                if shard[j] == i:
                    shard_members[i].append(view['list'][j])
        for x in range(0, numShards):
            shard_ids.append(str(x))
        if numberOfKeys != 0:
            reHashKeys()


shardNodes(numShards)
# TODO integrate data migration

# FUNCTION: build_payload
# DESCRIPTION: builds a payload dictionary with values
# inside key_value_db or a dummy payload if the key does
# not exist in the payload
def build_payload(key):
    dprint("Building payload with key: " + key)
    bPayload = {}
    if key in key_value_db:
        dprint(key + " exists")
        # get payload values from KVS
        bPayload["vc"] = key_value_db[key][KVS_VC_POS]
        bPayload["timestamp"] = key_value_db[key][KVS_TS_POS]
        dprint(bPayload)
    else:
        dprint(key + " does not exist")
        # create a dummy payload
        bPayload["vc"] = dummy_vector_clock()
        bPayload["timestamp"] = time.time()
        dprint('bpayload!:')
        dprint(bPayload)
        dprint('that wat the ting!:')
    return bPayload

# FUNCTION: dummy_vector_clock
# DECRIPTION: returns a dummy vector clock of values
# <0, 0, ....>  for all views
def dummy_vector_clock():
    nVC = {}
    for v in view['list']:
        nVC[v] = 0
    return nVC

# FUNCTION: increment_clock
# DESCRIPTION: increments the clock at node's ip
def increment_clock(vc):
    if my_ip in vc:
        vc[my_ip] += 1
        return True
    else:
        return False


def reHashKeys():
    print('this shoud do something')


def max(a, b):
    if a > b:
        return a
    return b


def storeKeyValue(ipPort, key, value, payload):
    return requests.put('http://%s/keyValue-store/%s' % (str(ipPort), key), data={'val': value, 'payload': json.dumps(payload)})


def deleteView(ipPort, view):
    del local_vc.clock[ipPort]
    return requests.delete('http://%s/view' % (str(ipPort)), data={'ip_port': view})


def addView(ipPort, view):
    local_vc.clock[ipPort] = 0
    return requests.put('http://%s/view' % (str(ipPort)), data={'ip_port': view})
    # return requests.put( 'http://%s/view'%str(ipPort), data={'ip_port':newAddress} )


def deleteB(ipPort, key, payload):
    return requests.delete('http://%s/keyValue-store/%s' % (str(ipPort), key), data={'payload': json.dumps(payload)})


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


def sendKey(ipPort, key, value, payload):
    dprint("SENDKEY: sending {%s: %s} to %s" % (key, value, ipPort))
    requests.put('http://%s/gossip/%s' % (str(ipPort), key),
                 data={'val': value, 'payload': json.dumps(payload)})


def sendView(ipPort, view):
    requests.put('http://%s/gossipView' %
                 (str(ipPort)), data={'view': json.dumps(view)})


def broadcastView(view):
    for ipPort in view['list']:
        if ipPort != my_ip:
            sendView(ipPort, view)


def broadcastStore(key, value, ts, vc, dflag):
    for ipPort in view['list']:
        if ipPort != my_ip:
            sendKey(ipPort, key, value, (ts, vc, dflag))


def less_than(vc1, vc2):
    all_equal = True
    for ip in view_list:
        if ip in vc1 and ip in vc2:
            if vc1[ip] < vc2[ip]:
                all_equal = False
            elif vc1[ip] > vc2[ip]:
                return False
    return not all_equal

# FUNCTION: isOlderThan
# DESCRIPTION: compares contexts (vector,timestamp)
#              returns True if a is older than b, false conversely
def isOlderThan(a, b):
    vcA, tsA = a
    vcB, tsB = b
    if less_than(vcA, vcB):
        return True
    if less_than(vcA, vcB):
        return False
    return tsA < tsB


class kvs_node(Resource):
    def handle_put(self, key, data, ts, vc, dflag):
        dprint("HANDLE_PUT")
        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg': 'Key not valid',
                'result': 'Error'
            }), status=400, mimetype=u'application/json')
        broadcastStore(key, data, ts, vc, dflag)
        if key not in key_value_db or key_value_db[key][KVS_DEL_POS] is True:
            key_value_db[key] = (data, ts, vc, dflag)
            nPayload = build_payload(key)
            return Response(json.dumps({
                'replaced': False,
                'msg': 'Added successfully',
                'payload': json.dumps(nPayload),
            }), status=200, mimetype=u'application/json')
        else:
            key_value_db[key] = (data, ts, vc, dflag)
            nPayload = build_payload(key)
            return Response(json.dumps({
                'replaced': True,
                'msg': 'Updated successfully',
                'payload': json.dumps(nPayload),
            }), status=201, mimetype=u'application/json')

    def get(self, key):
        dprint("GET")
        payload = request.form.get('payload')
        payload = ast.literal_eval(payload)
        dprint("check out this payload: %s" % payload)
        if len(payload) == 0:
            nVC = dummy_vector_clock()
            nTS = time.time()
        else:
            nVC = payload["vc"]
            nTS = payload["timestamp"]

        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg': 'Key not valid',
                'result': 'Error'}))

        # ays to return key does not exist if not in db
        # along with payload and 404 status
        if key not in key_value_db:
            return Response(json.dumps({
                'result': 'Error',
                'msg': 'Key does not exist',
                'payload': json.dumps({})
                }), status=404, mimetype=u'application/json')
        elif isOlderThan((key_value_db[key][KVS_VC_POS],key_value_db[key][KVS_TS_POS]),(nVC,nTS)):
                return Response(json.dumps({
                    'result':'Error',
                    'msg' : 'Payload out of date',
                    'payload': json.dumps(payload)
                    }), status=404, mimetype=u'application/json')
        elif key_value_db[key][KVS_DEL_POS] is True:
            return Response(json.dumps({
                'result':'Error',
                'msg' : 'Key does not exist',
                'payload': json.dumps(build_payload(key))
                }), status=404, mimetype=u'application/json')
        # Spec says to return value of key if key
        # in database along with payload with return status of 200
        return Response(json.dumps({
            'result' : 'Success',
            'value' : key_value_db[key][KVS_VAL_POS],
            'payload' :  json.dumps(build_payload(key)),
        }), status=200, mimetype=u'application/json')

    def delete(self, key):
        dprint("DELETE")
        payload = request.form.get('payload')
        payload = ast.literal_eval(payload)
        if len(payload) == 0:
            nVC = dummy_vector_clock()
            nTS = time.time()
        else:
            nVC = payload["vc"]
            nTS = payload["timestamp"]
        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg':'Key not valid',
                'result' : 'Error'}))
        if key not in key_value_db:
            return Response(json.dumps({
                'result':'Error',
                'msg':'Key does not exist',
                'payload': json.dumps(payload)}),
                status=404, mimetype=u'application/json')
        elif isOlderThan((nVC,nTS),(key_value_db[key][KVS_VC_POS],key_value_db[key][KVS_TS_POS])):
            return Response(json.dumps({
                'result':'Error',
                'msg' : 'Payload out of date',
                'payload': json.dumps(build_payload(key))
                }), status=404, mimetype=u'application/json')
        elif key_value_db[key][KVS_DEL_POS] is True:
            return Response(json.dumps({
                'result': 'Error',
                'msg': 'Key does not exist',
                'payload': json.dumps(payload)}),
                status=404, mimetype=u'application/json')
        # del key_value_db[key]
        val, ts, vc, dflag = key_value_db[key]
        increment_clock(vc)
        self.handle_put(key, val, ts, vc, True)
        return Response(json.dumps({
            'result': 'Success',
            'msg': 'Key deleted',
            'payload': json.dumps(build_payload(key)),
            }),
            status=200, mimetype=u'application/json')

    def put(self, key):
        dprint("PUT")
        # Value to put in kvs
        value = request.form.get('val')
        # Payload containing additional information:
        # timestamp, key vector clock

        payload = ast.literal_eval(request.form.get('payload'))

        # get the vector_clock from the payload
        if len(payload) == 0:
            nVC = dummy_vector_clock()
            nTS = time.time()
        else:
            nVC = payload["vc"]
            nTS = payload["timestamp"]

        # increment the vector_clock for my_ip
        increment_clock(nVC)

        # get key's vector_clock and timestamp
        if(key in key_value_db):
            kVC = key_value_db[key][KVS_VC_POS]
            kTS = key_value_db[key][KVS_TS_POS]
        else:
            # set both to equal so that overwrite will return true
            kVC = dummy_vector_clock()
            kTS = nTS

        # test to see if value should be overwritten
        if isOlderThan((kVC, kTS),(nVC,nTS)):
            return self.handle_put(key, value, nTS, nVC, False)

class kvs_search(Resource):
  def get(self, key):

    dprint("kvs_search - get")
    # create payload
    nPayload = build_payload(key)

    if key not in key_value_db or key_value_db[key][KVS_DEL_POS] is True:
        return Response(json.dumps({
            'isExists': False,
            'result':'Success',
            'payload' :  json.dumps(nPayload)}),
            status=200, mimetype=u'application/json')
    else:
        return Response(json.dumps({
            'isExists': True,
            'result':'Success',
            'payload':  json.dumps(nPayload)}),
            status=200, mimetype=u'application/json')

class kvs_view(Resource):
    def get(self):
        dprint("KVS_VIEW - GET")
        return Response(json.dumps({
            'view': ",".join(view['list']),
        }),
        status=200, mimetype=u'application/json')

    def delete(self):
        dprint("KVS_VIEW - DELETE")
        # I hope all we need to check for is the port sent
        ip_port = request.form.get('ip_port')
        if ip_port not in view['list']:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : request.form.get('ip_port') + " is not in current view"
            }),
            status=404, mimetype=u'application/json')
        else:
            view['list'].remove(ip_port)
            view['updated'] = time.time()
            broadcastView(view)
            # broadcastViewDelete(ip_port)
            return Response(json.dumps({
                'result' : 'Success',
                'msg' : 'Successfully removed ' + request.form.get('ip_port') + ' from view'
            }),
            status=200, mimetype=u'application/json')

    def put(self):
        dprint("KVS_VIEW - PUT")
        ip_port = request.form.get('ip_port')
        if ip_port in view['list']:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : request.form.get('ip_port') + ' is already in view'
            }),
            status=404, mimetype=u'application/json')
        else:
            # I will attempt to implement the view change.
            view_clock = time.time()
            view['list'].append(ip_port)
            view['list'].sort()
            view['updated'] = time.time()
            broadcastView(view)
            return Response(json.dumps({
                'result' : 'Success',
                'msg' : 'Successfully added ' + request.form.get('ip_port') + ' to view'
            }),
            status=200, mimetype=u'application/json')


# Disemination of key-values between nodes (Gossip of data)
class dis_kvs(Resource):

    def put(self, key):
        value = request.form.get('val')
        payload = request.form.get('payload')
        ts, vc, dflag = json.loads(payload)
        # dprint("PUT Ing from %s : %s" %(request.remote_addr,json.loads(payload)))

        if key in key_value_db:
            kVC = key_value_db[key][KVS_VC_POS]
            kTS = key_value_db[key][KVS_TS_POS]
        else:
            kVC = dummy_vector_clock()
            kTS = ts
        if isOlderThan((kVC,kTS),(vc,ts)):
            key_value_db[key] = (value, ts, vc, dflag)

# Disemination of views between nodes (Gossip of views)
class dis_view(Resource):

    def put(self):
        msg_view = ast.literal_eval(request.form.get('view'))
        if view['updated'] < msg_view['updated']:
            # dprint("AND it looks like we're gonna have to do an update thingy bc my clock is at %s"% view['updated'])
            # dprint("%s is less than %s" %( view['updated'],msg_view['updated']))
            view['list'] = copy.deepcopy(msg_view['list'])
            view['updated'] = msg_view['updated']
            # dprint('this is my view now: %s' % view)

class kvs_shard_my_id(Resource):
    def get(self):
        return Response(json.dumps({
            'id' : shardID
        }),
        status=200, mimetype=u'application/json')

class kvs_shard_all_ids(Resource):
    def get(self):
        return Response(json.dumps({
            'result' : 'Success',
            'shard_ids' : ",".join(shard_ids)
        }),
        status=200, mimetype=u'application/json')

class kvs_shard_members(Resource):
    def get(self, input_id):
        if input_id in shard_ids:
            return Response(json.dumps({
                'result' : 'Success',
                'members' : ",".join(str(x) for x in shard_members[int(input_id)])
            }),
            status=200, mimetype=u'application/json')
        else:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'No shard with id ' + input_id
            }),
            status=404, mimetype=u'application/json')

class kvs_shard_count(Resource):
    def get(self, input_id):
        if input_id in shard_ids:
            return Response(json.dumps({
                'result' : 'Success',
                'Count' : numberOfKeys
            }),
            status=200, mimetype=u'application/json')
        else:
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'No shard with id ' + input_id
            }),
            status=404, mimetype=u'application/json')

class kvs_shard_changeShardNumber(Resource):
    def put(self):
        newNumber = request.form.get('num')
        if newNumber == '0':
          return Response(json.dumps({
              'result' : 'Error',
              'msg' : 'Must have at lease one shard'
          }),
          status=400, mimetype=u'application/json')
        elif int(newNumber) <= numShards: 
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'Not enough nodes for ' + newNumber + ' shards'
            }),
            status=400, mimetype=u'application/json')
        elif ((len(view['list']))/2) < numShards: 
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'Not enough nodes. ' + newNumber + ' shards result in a nonfault tolerant shard'
            }),
            status=400, mimetype=u'application/json')
        else: 
            #TODO broadcast resharding
            shardNodes(int(newNumber))
            return Response(json.dumps({
              'result' : 'Success',
              'shard_ids' : shard_ids
            }),
            status=200, mimetype=u'application/json')



api.add_resource(kvs_node, '/keyValue-store/<string:key>')
api.add_resource(kvs_search, '/keyValue-store/search/<string:key>')
api.add_resource(kvs_view, '/view')
api.add_resource(kvs_shard_my_id, '/shard/my_id')
api.add_resource(kvs_shard_all_ids, '/shard/all_ids')
api.add_resource(kvs_shard_members, '/shard/members/<string:input_id>')
api.add_resource(kvs_shard_count, '/shard/count/<string:input_id>')
api.add_resource(kvs_shard_changeShardNumber, '/shard/changeShardNumber')
api.add_resource(dis_kvs, '/gossip/<string:key>')
api.add_resource(dis_view, '/gossipView')

dprint("Viewlist upon startup: %s"% view['list'])
threading.Thread(target=gossip_kvs).start()
threading.Thread(target=gossip_view).start()
view_clock = time.time()

print("ip and port = %s" %my_ip)

# local_vc = vector_clock()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
