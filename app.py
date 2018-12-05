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
view_list.sort()
my_ip = os.environ.get('IP_PORT')
#initial number of shards
numShards = int(os.environ.get('S'))

#list of all shards in the system
shard_ids = []
for x in range (1, numshards):
  shard_ids.append(x)

#number of key-value pairs this shard is responsible for 
numberOfKeys = 0

#this node's shard
shardID = 0

#list of all this shard's members as IP addresses
shard_members = []

shardNodes(numShards, view_list, numberOfKeys)

def shardNodes(shardSize, nodeList, numKeys):
    #if not enough nodes to shard into specified size, default to 1
    #NOTE: this is only for initialization, not manual view/shard changes
    if ((view_list.len() + 1)/2) < shardSize:
        shardSize = 1
        shardID = 0
        shard_members = nodeList
    #if changing to one shard 
    elif shardSize == 1: 
        shardSize = 1
        shardID = 0
        shard_members = nodeList
        #check if this node needs to migrate data
        if numberOfKeys != 0:
            reHashKeys()

    else:
        shards = []
        for i in range(0, len(view_list) - 1):
            shard[view_list[i]] = i % shardSize
        shardID = shard[my_ip]
        for i in range(0, len(view_list) - 1):
            if shard[view_list[i]] == shardID:
                shard_members.append(view_list[i])
        if numberOfKeys != 0:
            reHashKeys()


#TODO integrate data migration
def reHashKeys():
    print('this shoud do something')
    
def dprint(msg):
    print(msg, file=sys.stderr)

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


def less_than(vc1, vc2):
    all_equal = True
    for ip in view_list:
        if ip in vc1 and ip in vc2:
            if vc1[ip] < vc2[ip]:
                all_equal = False
            elif vc1[ip] > vc2[ip]:
                return False
    return not all_equal

def overwrite(vc1, ts1, vc2, ts2):
    if less_than(vc1, vc2):
        return True
    if less_than(vc2, vc1):
        return False
    return ts1 > ts2

class kvs_node(Resource):
    def handle_put(self, key, data, ts, vc):

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
            key_value_db[key] = (data, ts, vc)
            return Response(json.dumps({
                'replaced': False,
                'msg': 'Added successfully',
                'payload' : {},
            }), status=200, mimetype=u'application/json')

        else:
            key_value_db[key] = (data, ts, vc)
            return Response(json.dumps({
                'replaced': True,
                'msg': 'Updated successfully',
                'payload' : {},
            }), status=201, mimetype=u'application/json')

    def get(self, key):
        dprint("Doing GET #1\n")
        if len(key) > 200 or len(key) < 1:
            return Response(json.dumps({
                'msg':'Key not valid',
                'result' : 'Error'}))

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
        hop_list = []#list(ast.literal_eval(request.form.get('payload')))
        dprint("DELETE hop_list:%s" % hop_list)
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
        broadcastDelete(key, hop_list)
        return Response(json.dumps({
            'result':'Success',
            'msg':'Key deleted',
            'payload' :  hop_list,
            }),
            status=200, mimetype=u'application/json')

    def put(self, key):
        # Value to put in kvs
        value = request.form.get('val')
        # Payload containing additional information:
        # hop_list, timestamp, key vector clock
        payload = request.form.get('payload')

        # DEBUG: printing the payload delivered
        print("\nPayload is type: %s\nPayload is: %s\n" %(type(payload),payload), file=sys.stderr)

        # get the hop_list from the payload to be compared
        hop_list = list(ast.literal_eval(payload))
        dprint(hop_list)
        # get the vector_clock from the payload
        # TODO: nVC = <get from payload>
        # dprint(nVC)
        # get the timestamp from the payload
        # TODO: nTS = <get from payload>
        # dprint(nTS)

        # only act on the payload if it has not already been seen yet
        if my_ip not in hop_list:

            # add ip to hop list (don't process again)
            hop_list.append(my_ip)

            # get key's vector_clock and timestamp
            if(key in key_value_db):
                kVC = key_value_db[key][2]
                kTS = key_value_db[key][1]
            else:
                kVC = nVC
                kTS = nTS

            # check if the value should be put
            # value VC is greater than
            #if VC_is_greater_than(nVC, kVC):
                # update the KVS                
            # value VC is less than
            #if VC_is_less_than(nVC, kVC):
                # ignore, the new value is old
            # value VC is equal to
            #if vc_is_equal_to(nVC, kVC):
                # do nothing, it is the same value
            #else:
                # clocks are concurrent but not equal
                # compare timestamps to see who is newer
                #if nTS > kTS:
                    # update the KVS
                #else:
                    # ignore, the new value is old

            temp = self.handle_put(key, value, nTS, nVC)
            dprint("Not in hop list, rebroadcasting")
            broadcast(key, value, hop_list)
            return temp
        # ignore if it has already been handled previously
        else:
            dprint("Already in hop list")
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
        dprint("Doing GET #2\n")
        search_vc = vector_clock()
        dprint("New VC")
        dprint(search_vc.clock)
        search_vc.clock = copy.deepcopy(local_vc.clock)
        dprint("init VC")
        dprint(search_vc.clock)
        search_vc.increment_clock()
        dprint("incremented VC")
        dprint(search_vc.clock)
        dprint("Got to here after GET #2")
        dprint(local_vc.clock)

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
            view_list.sort()
            broadcastViewAdd(ip_port)
            for k,v in key_value_db.items():
                storeKeyValue(ip_port, k, v, local_vc.to_dict())
            print("newthing: %s" % view_list,file=sys.stderr)
            return Response(json.dumps({
                'result' : 'Success',
                'msg' : 'Successfully added ' + request.form.get('ip_port') + ' to view'
            }),
            status=200, mimetype=u'application/json')

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
                'members' : shard_members
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
        elif True: ##TODO propogate shard redistribution, return if succeeds
            return Response(json.dumps({
              'result' : 'Success',
              'shard_ids' : shard_ids
            }),
            status=200, mimetype=u'application/json')
        elif True: #TODO if newNumber is greater than # of nodes in the view 
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'Not enough nodes for ' + newNumber + ' shards'
            }),
            status=400, mimetype=u'application/json')
        else: #TODO if there is only 1 node in any partition after redividing, abort
            return Response(json.dumps({
                'result' : 'Error',
                'msg' : 'Not enough nodes. ' + newNumber + ' shards result in a nonfault tolerant shard'
            }),
            status=400, mimetype=u'application/json')



api.add_resource(kvs_node, '/keyValue-store/<string:key>')
api.add_resource(kvs_search, '/keyValue-store/search/<string:key>')
api.add_resource(kvs_view, '/view')
api.add_resource(kvs_shard_my_id, '/shard/my_id')
api.add_resource(kvs_shard_all_ids, '/shard/all_ids')
api.add_resource(kvs_shard_members, '/shard/members/<string:input_id>')
api.add_resource(kvs_shard_count, '/shard/count/<string:input_id>')
api.add_resource(kvs_shard_changeShardNumber, '/shard/count/changeShardNumber')

print("ip and port = %s" %my_ip)

local_vc = vector_clock()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
