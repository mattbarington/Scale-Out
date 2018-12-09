import os
import sys
import requests
import time
import unittest
import json

import docker_control

import io

dockerBuildTag = "testing" #put the tag for your docker build here

hostIp = "localhost"

needSudo = False # obviously if you need sudo, set this to True
#contact me imediately if setting this to True breaks things
#(I don't have a machine which needs sudo, so it has not been tested, although in theory it should be fine)

port_prefix = "808"

networkName = "mynetwork" # the name of the network you created

networkIpPrefix = "192.168.0." # should be everything up to the last period of the subnet you specified when you
# created your network

propogationTime = 3 #sets number of seconds we sleep after certain actions to let data propagate through your system
# you may lower this to speed up your testing if you know that your system is fast enough to propigate information faster than this
# I do not recomend increasing this

dc = docker_control.docker_controller(networkName, needSudo)

def getViewString(view):
    listOStrings = []
    for instance in view:
        listOStrings.append(instance["networkIpPortAddress"])

    return ",".join(listOStrings)

def viewMatch(collectedView, expectedView):
    collectedView = collectedView.split(",")
    expectedView = expectedView.split(",")

    if len(collectedView) != len(expectedView):
        return False

    for ipPort in expectedView:
        if ipPort in collectedView:
            collectedView.remove(ipPort)
        else:
            return False

    if len(collectedView) > 0:
        return False
    else:
        return True

def storeKeyValue(ipPort, key, value, payload):
    print('PUT: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.put('http://%s/keyValue-store/%s' % (str(ipPort), key), data={'val': value, 'payload': json.dumps(payload)}, timeout=5)

def checkKey(ipPort, key, payload):
    #print('GET: http://%s/keyValue-store/search/%s'%(str(ipPort), key))
    return requests.get( 'http://%s/keyValue-store/search/%s'%(str(ipPort), key), data={'payload': json.dumps(payload)} )

def getKeyValue(ipPort, key, payload):
    #print('GET: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.get( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': json.dumps(payload)} )

def deleteKey(ipPort, key, payload):
    #print('DELETE: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.delete( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': json.dumps(payload)} )

    #Replication Functions
def addNode(ipPort, newAddress):
    #print('PUT: http://%s/view'%str(ipPort))
    return requests.put( 'http://%s/view'%str(ipPort), data={'ip_port':newAddress} )

def removeNode(ipPort, oldAddress):
    #print('DELETE: http://%s/view'%str(ipPort))
    return requests.delete( 'http://%s/view'%str(ipPort), data={'ip_port':oldAddress} )

def viewNetwork(ipPort):
    #print('GET: http://%s/view'%str(ipPort))
    return requests.get( 'http://%s/view'%str(ipPort) )

def getShardId(ipPort):
    return requests.get( 'http://%s/shard/my_id'%str(ipPort) )

def getAllShardIds(ipPort):
    return requests.get( 'http://%s/shard/all_ids'%str(ipPort) )

def getMembers(ipPort, ID):
    return requests.get( 'http://%s/shard/members/%s'%(str(ipPort), str(ID)) )

def getCount(ipPort, ID):
    return requests.get( 'http://%s/shard/count/%s'%(str(ipPort), str(ID)) )

def changeShardNumber(ipPort, newNumber):
    return requests.put( 'http://%s/shard/changeShardNumber'%str(ipPort), data={'num' : newNumber} ) 

###########################################################################################

class TestHW4(unittest.TestCase):
    view = {}

    def setUp(self):
        self.view = dc.spinUpManyContainers(dockerBuildTag, hostIp, networkIpPrefix, port_prefix, 6, 3)

        for container in self.view:
            if " " in container["containerID"]:
                self.assertTrue(False, "There is likely a problem in the settings of your ip addresses or network.")

        #dc.prepBlockade([instance["containerID"] for instance in self.view])

    def tearDown(self):
        dc.cleanUpDockerContainer()
        #dc.tearDownBlockade()

    def getPayload(self, ipPort, key):
        response = checkKey(ipPort, key, {})
        #print(response)
        data = response.json()
        return data["payload"]

    def partition(self, partitionList):
        truncatedList = []
        for partition in partitionList:
            truncatedPartition = []
            for node in partition:
                truncatedPartition.append(node[:12])
            truncatedPartition = ",".join(truncatedPartition)
            truncatedList.append(truncatedPartition)
        dc.partitionContainer(truncatedList)

    def partitionAll(self):
        listOLists = []
        for node in self.view:
            listOLists.append([node["containerID"]])
        self.partition(listOLists)

    def confirmAddKey(self, ipPort, key, value, expectedStatus, expectedMsg, expectedReplaced, payload={}):
        response = storeKeyValue(ipPort, key, value, payload)

        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['msg'], expectedMsg)
        self.assertEqual(data['replaced'], expectedReplaced)

        return data["payload"]

    def confirmCheckKey(self, ipPort, key, expectedStatus, expectedResult, expectedIsExists, payload={}):
        response = checkKey(ipPort, key, payload)
        #print(response)
        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        self.assertEqual(data['isExists'], expectedIsExists)

        return data["payload"]

    def confirmGetKey(self, ipPort, key, expectedStatus, expectedResult, expectedValue=None, expectedOwner=None, expectedMsg=None, payload={}):
        response = getKeyValue(ipPort, key, payload)
        #print(response)
        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        if expectedValue != None and 'value' in data:
            self.assertEqual(data['value'], expectedValue)
        if expectedMsg != None and 'msg' in data:
            self.assertEqual(data['msg'], expectedMsg)
        if expectedOwner != None and 'owner' in data:
            self.assertEqual(data["owner"], expectedOwner)

        return data["payload"]

    def confirmDeleteKey(self, ipPort, key, expectedStatus, expectedResult, expectedMsg, payload={}):
        response = deleteKey(ipPort, key, payload)
        #print(response)

        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        self.assertEqual(data['msg'], expectedMsg)

        return data["payload"]

    def confirmViewNetwork(self, ipPort, expectedStatus, expectedView):
        response = viewNetwork(ipPort)
        #print(response)
        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()

        self.assertTrue(viewMatch(data['view'], expectedView), "%s != %s"%(data['view'], expectedView))

    def confirmAddNode(self, ipPort, newAddress, expectedStatus, expectedResult, expectedMsg):
        response = addNode(ipPort, newAddress)

        #print(response)

        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        self.assertEqual(data['msg'], expectedMsg)

    def confirmDeleteNode(self, ipPort, removedAddress, expectedStatus, expectedResult, expectedMsg):
        response = removeNode(ipPort, removedAddress)
        #print(response)
        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        self.assertEqual(data['msg'], expectedMsg)

    def checkGetMyShardId(self, ipPort, expectedStatus=200):
        response = getShardId(ipPort)

        self.assertEqual(response.status_code, expectedStatus)
        data = response.json()
        self.assertTrue('id' in data)

        return str(data['id'])

    def checkGetAllShardIds(self, ipPort, expectedStatus=200):
        response = getAllShardIds(ipPort)

        self.assertEqual(response.status_code, expectedStatus)
        data = response.json()
        return data["shard_ids"].split(",")

    def checkGetMembers(self, ipPort, ID, expectedStatus=200, expectedResult="Success", expectedMsg=None):
        response = getMembers(ipPort, ID)

        self.assertEqual(response.status_code, expectedStatus)
        data = response.json()

        self.assertEqual(data['result'], expectedResult)

        if "msg" in data and expectedMsg == None:
            self.assertEqual(data['msg'], expectedMsg)
        else:
            return data["members"].split(",")

    def getShardView(self, ipPort):
        shardIDs = self.checkGetAllShardIds(ipPort)
        shardView = {}
        for ID in shardIDs:
            shardView[ID] = self.checkGetMembers(ipPort, ID)
        return shardView

    def checkConsistentMembership(self, ipPort, ID):
        shard = self.checkGetMembers(ipPort, ID)
        for member in shard:
            self.assertEqual(self.checkGetMyShardId(member), ID)

    def checkChangeShardNumber(self, ipPort, newNumber, expectedStatus, expectedResult, expectedShardIds, expectedMsg=""):
        response = changeShardNumber(ipPort, str(newNumber))

        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)

        if expectedMsg:
            self.assertEqual(data['msg'], expectedMsg)
        else:
            self.assertEqual(data['shard_ids'], expectedShardIds)

    def checkGetCount(self, ipPort, ID, expectedStatus, expectedResult, expectedCount):
        response = getCount(ipPort, ID)

        self.assertEqual(response.status_code, expectedStatus)

        data = response.json()
        self.assertEqual(data['result'], expectedResult)
        self.assertEqual(data['Count'], expectedCount)

##########################################################################
## Tests start here ##
##########################################################################

    # check that they do things,
    # not that they do the right thing,
    # just that they don't return an error
    def test_a_shard_endpoints(self):
        print("TEST A: SHARD ENDPOINTS")
        ipPort = self.view[0]["testScriptAddress"]

        ID = self.checkGetMyShardId(ipPort)
        self.checkGetAllShardIds(ipPort)
        self.checkGetMembers(ipPort, ID)
        self.getShardView(ipPort)

    # check everyone agrees about who is where
    def test_b_shard_consistent_view(self):
        print("TEST B: SHARD CONSISTENT VIEW")
        ipPort = self.view[0]["testScriptAddress"]

        shardView = self.getShardView(ipPort)
        for ID in shardView.keys():
            self.checkConsistentMembership(ipPort, ID)

    # no node is alone in a shard
    def test_c_shard_no_lonely_nodes(self):
        print("TEST C: NO LONELY NODES")
        ipPort = self.view[0]["testScriptAddress"]

        shardView = self.getShardView(ipPort)
        for shard in shardView:
            length = len(shardView[shard])
            self.assertTrue(length > 1)

    # number of shards should not change
    def test_d_shard_add_node(self):
        print("TEST D: SHARD ADD NODE")
        ipPort = self.view[0]["testScriptAddress"]

        initialShardIDs = self.checkGetAllShardIds(ipPort)

        newPort = "%s8"%port_prefix
        newView = "%s8:8080"%(networkIpPrefix)

        viewSting = getViewString(self.view)
        viewSting += ",%s"%newView
        newNode = dc.spinUpDockerContainer(dockerBuildTag, hostIp, networkIpPrefix+"8", newPort, viewSting, 3)

        self.confirmAddNode(ipPort=ipPort,
                            newAddress=newView,
                            expectedStatus=200,
                            expectedResult="Success",
                            expectedMsg="Successfully added %s to view"%newView)

        time.sleep(propogationTime)
        newShardIDs = self.checkGetAllShardIds(ipPort)

        self.assertEqual(len(newShardIDs), len(initialShardIDs))

    # removing a node decrease number of shards
    def test_e_shard_remove_node(self):
        print("TEST E: SHARD REMOVE NODE")
        ipPort = self.view[0]["testScriptAddress"]
        removedNode = self.view.pop()["networkIpPortAddress"]

        initialShardIDs = self.checkGetAllShardIds(ipPort)

        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        newShardIDs = self.checkGetAllShardIds(ipPort)

        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

    # Test Case: x1
    # Description: 
    # - Starts at 6 node, 3 shard.
    # - Removes node
    # - Check for sucessful resharding (2 shard)
    # - Reshard to 3 shards
    # - Check to make sure shard change was rejected
    # - Add node
    # - Check for still at 2 shards
    # - Reshard to 3 shards
    # - Check to make shard change was accepted
    def test_x1_reshard_auto_reject_accept(self):

        print("TEST X1: Reshards [auto, reject, accept]")
        ipPort = self.view[0]["testScriptAddress"]
        print(self.view)
        initialShardIDs = self.checkGetAllShardIds(ipPort)

        print("Initial Shard IDs")
        print(initialShardIDs)
        
        # remove a node, confirm it's removal
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting 3 sec...")

        time.sleep(propogationTime)

        # check for 2 shards
        print("Checking for auto-resharding to 2 shards")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # attempt to add shard (should be rejected)
        print("Attempting to change to 3 shards (should be rejected)")
        self.checkChangeShardNumber(ipPort=ipPort,
                               expectedStatus=400,
                               expectedResult="Error",
                               newShardNumber=3)

        # add a node back, confirm its addition
        print("Adding a node back")
        initialShardIDs = newShardIDs
        newPort = "%s8"%port_prefix
        newView = "%s8:8080"%(networkIpPrefix)

        viewSting = getViewString(self.view)
        viewSting += ",%s"%newView
        newNode = dc.spinUpDockerContainer(dockerBuildTag, hostIp, networkIpPrefix+"8", newPort, viewSting, 3)

        self.confirmAddNode(ipPort=ipPort,
                            newAddress=newView,
                            expectedStatus=200,
                            expectedResult="Success",
                            expectedMsg="Successfully added %s to view"%newView)

        print("waiting 3 sec...")
        time.sleep(propogationTime)
        
        # check to ensure the shard number hasn't changed
        print("Ensuring the shard number is still 2")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # attempt to add shard (should be accepted)
        print("Now attempting to change shard number to 3 (acceptable)")
        self.checkChangeShardNumber(ipPort=ipPort,
                               expectedStatus=200,
                               expectedResult="Success",
                               newShardNumber=3)

    # Test Case: x2
    # Description: 
    # - Start at 6 node, 3 shard
    # - remove node
    # - check for 2 shard
    # - remove node
    # - check for 2 shard
    # - remove node
    # - check for 1 shard
    # - remove node
    # - check for 1 shard
    # - remove node
    # - check for 1 shard
    def test_x2_remove_nodes_down_to_one(self):

        print("TEST X2: REMOVE NODES DOWN TO ONE")

        ipPort = self.view[0]["testScriptAddress"]
        initialShardIDs = self.checkGetAllShardIds(ipPort)

        print(self.view[0]["containerID"])

        print("Removing a node")
        # remove a node, confirm it's removal
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        # check for 2 shards (five nodes left)
        print("Ensure auto reshard to 2 shards (5 nodes)")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # remove a node, confirm it's removal
        print("Removing a node")
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        # check for 2 shards (four nodes left)
        print("Checking still at 2 shards (4 nodes)")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # remove a node, confirm it's removal
        print("Removing another node")
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        # check for 1 shard (3 nodes left)
        print("Ensure auto reshard to 1 shard (3 nodes)")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # remove a node, confirm it's removal
        print("remove another node")
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        # check for 1 shard (2 nodes left)
        print("ensure still 1 shard (2 nodes)")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # remove a node, confirm it's removal
        print("remove a node")
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        # check for 1 shard (1 node left)
        print("ensure still 1 shard (1 node)")
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

    def test_x3_add_keys_test_hash(self):

        print("TEST X3: ADD KEYS TEST HASH")
       
        print(self.view)

        ipPort = self.view[0]["testScriptAddress"]
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        
        all_vals = ["joey", "matt", "parker", "ryan", "peter", \
                "hpg", "ocelot", "america", "film", "echo", "ashton", \
                "jeep", "stick", "mind", "boi", "test"]

        all_key_vals = {}
        local_shard_count = [0,0,0]

        for key in all_vals:
            thisShard = hash(key) % 3
            local_shard_count[thisShard] = local_shard_count[thisShard] + 1
            all_key_vals[key] = str(thisShard)

        for k in all_key_vals:

            #print("key: %s "%k)
            #print("val: %s "%all_key_vals[k])
 
            payload = self.getPayload(ipPort, k)

            payload = self.confirmAddKey(ipPort=ipPort,
                           key=k,
                           value=all_key_vals[k],
                           expectedStatus=200,
                           expectedMsg="Added successfully",
                           expectedReplaced=False,
                           payload= payload)

        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        for i in range(6):
            print(i)
            curIP = self.view[i]["testScriptAddress"]
            response = getShardId(curIP)
            data = response.json()
            sID = data["id"]
            #print("shard ID: %s" % sID)
            response = getCount(curIP, sID)
            data = response.json()
            cnt = data["Count"]
            
            print("db: " + str(cnt) + " local: " + str(local_shard_count[sID]))
            self.assertEqual(cnt, local_shard_count[sID])


    def test_z_add_key_value_one_node(self):

        print("TEST Z: ADD KEY VALUE ONE NODE")

        ipPort = self.view[0]["testScriptAddress"]
        key = "addNewKey"

        payload = self.getPayload(ipPort, key)

        payload = self.confirmAddKey(ipPort=ipPort,
                           key=key,
                           value="a simple value",
                           expectedStatus=200,
                           expectedMsg="Added successfully",
                           expectedReplaced=False,
                           payload= payload)

        value = "aNewValue"

        payload = self.confirmAddKey(ipPort=ipPort,
                           key=key,
                           value=value,
                           expectedStatus=201,
                           expectedMsg="Updated successfully",
                           expectedReplaced=True,
                           payload=payload)

        payload = self.confirmCheckKey(ipPort=ipPort,
                            key=key,
                            expectedStatus=200,
                            expectedResult="Success",
                            expectedIsExists=True,
                           payload=payload)

        payload = self.confirmGetKey(ipPort=ipPort,
                           key=key,
                           expectedStatus=200,
                           expectedResult="Success",
                           expectedValue=value,
                           payload=payload)

     # change S from 3 to 2 using changeShardNumber endpoint
    def test_f_decrease_shard(self):
        print("TEST F: decrease shard from 3 to 2")
        ipPort = self.view[0]["testScriptAddress"]
        targetNode = self.view[-1]["networkIpPortAddress"]

        initialShardIDs = self.checkGetAllShardIds(ipPort)

        self.checkChangeShardNumber(targetNode, 2, 200, "Success", "0,1")
        time.sleep(propogationTime)

        self.assertEqual(2, len(initialShardIDs)-1)

    # removing 1 node from shard with 2 nodes result number of shards 
    # to decrease and lonely node to join other shard
    def test_g_remove_node_causes_shard_decrease(self):
        print("TEST G: remove node decrease shard")
        ipPort = self.view[0]["testScriptAddress"]
        removedNode = self.view.pop()["networkIpPortAddress"]
        targetNode = self.view[-1]["networkIpPortAddress"]

        self.confirmDeleteNode(ipPort=ipPort, 
                               removedAddress=removedNode, 
                               expectedStatus=200, 
                               expectedResult="Success", 
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        #❗️check first shard (shard id might be different dependending on how you redestribute the nodes)
        members = self.checkGetMembers(ipPort, 0)

        lonelyNodeInFirstShard = targetNode in members

        self.assertEqual(True, lonelyNodeInFirstShard)

    # changing shard size to 1 causes all nodes to be in that only shard
    def test_h_change_shard_size_to_one(self):
        print("TEST H: change shard size to one")
        ipPort = self.view[0]["testScriptAddress"]

        self.checkChangeShardNumber(ipPort, 1, 200, "Success", "0")

        time.sleep(propogationTime)

        members = self.checkGetMembers(ipPort, 0)

        # check if all members are present
        for view in self.view:
            currIpInShard = view['networkIpPortAddress'] in members
            self.assertEqual(True, currIpInShard)

    # changing shard size from 1 to 2 should have 3 members in each shard
    def test_i_change_shard_size_from_one_to_two(self):
        print("TEST I increase shard size from 1 to 2")
        self.test_h_change_shard_size_to_one()

        ipPortOne = self.view[0]["testScriptAddress"]
        ipPortTwo = self.view[1]["testScriptAddress"]

        members = self.checkGetMembers(ipPortOne, 0)
        membersTwo = self.checkGetMembers(ipPortTwo, 0)

        self.checkChangeShardNumber(ipPortOne, 2, 200, "Success", "0,1")

        membersOne = self.checkGetMembers(ipPortOne, 0)
        membersTwo = self.checkGetMembers(ipPortTwo, 0)

        self.assertEqual(3, len(membersOne))
        self.assertEqual(3, len(membersTwo))

    # when shard decreased and an isolated node moved to another shard, 
    # its keys get shared to rest of shard members, and the key's owner is 
    # consistent with new shard id
    def test_j_key_redistributed(self):
        print("TEST J key redistribution")
        ipPort = self.view[0]["testScriptAddress"]
        removedNode = self.view.pop()["networkIpPortAddress"]
        targetNode = self.view[-1]["networkIpPortAddress"]

        self.confirmAddKey(targetNode, 'key1', 'value1', 201, "Added successfully", False, {})

        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200, 
                               expectedResult="Success", 
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        #❗️again, expected owner might be different based on different shard mechanic
        # we use regular hashing and sha1 as hash function -> hash(key1) % 2 = 1
        self.confirmGetKey(targetNode, 'key1', 200, "Success", 'value1', "1")

        self.confirmGetKey(ipPort, 'key1', 200, "Success", 'value1', "1")

    # setting shard to <=0 should be invalid
    def test_k_set_shard_to_zero(self):
        print("TEST k shard to 0")
        ipPort = self.view[0]["testScriptAddress"]
        self.checkChangeShardNumber(ipPort, 0, 400, "Error", "", "Must have at least one shard")
    
    def test_zyy_add_key_value_then_reshard(self):

        print("TEST ZYY: add keys then reshard")

        ipPort = self.view[0]["testScriptAddress"]
        key = "addNewKey"
        
        initialShardIDs = self.checkGetAllShardIds(ipPort)

        print("Initial Shard IDs (should be 0, 1, 2)")
        print(initialShardIDs)
        payload = self.getPayload(ipPort, key)

        payload = self.confirmAddKey(ipPort=ipPort,
                           key=key,
                           value="a simple value",
                           expectedStatus=200,
                           expectedMsg="Added successfully",
                           expectedReplaced=False,
                           payload= payload)

        value = "aNewValue"

        #CRASHES HERE BOYS
        payload = self.confirmAddKey(ipPort=ipPort,
                           key=key,
                           value=value,
                           expectedStatus=201,
                           expectedMsg="Updated successfully",
                           expectedReplaced=True,
                           payload=payload)
        

        print("changing shard number to 2")
        self.checkChangeShardNumber(ipPort=ipPort,
                               expectedStatus=200,
                               expectedResult="Success",
                               newShardNumber=2)
        print("waiting for 3 seconds...")
        time.sleep(propogationTime)

        newIDs = self.checkGetAllShardIds(ipPort)

        print("new Shard IDs (should be 0, 1)")
        print(newIDs)

        print("checking if data was rehashed properly")
        payload = self.confirmCheckKey(ipPort=ipPort,
                            key=key,
                            expectedStatus=200,
                            expectedResult="Success",
                            expectedIsExists=True,
                           payload=payload)

        payload = self.confirmGetKey(ipPort=ipPort,
                           key=key,
                           expectedStatus=200,
                           expectedResult="Success",
                           expectedValue=value,
                           payload=payload)


if __name__ == '__main__':
    unittest.main()
