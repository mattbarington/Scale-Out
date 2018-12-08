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

# Basic Functionality
# These are the endpoints we should be able to hit
    #KVS Functions
def storeKeyValue(ipPort, key, value, payload):
    #print('PUT: http://%s/keyValue-store/%s'%(str(ipPort), key))
    print("Sending out payload: %s"%payload)
    return requests.put( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'val':value, 'payload': payload})

def checkKey(ipPort, key, payload):
    #print('GET: http://%s/keyValue-store/search/%s'%(str(ipPort), key))
    return requests.get( 'http://%s/keyValue-store/search/%s'%(str(ipPort), key), data={'payload': payload} )

def getKeyValue(ipPort, key, payload):
    #print('GET: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.get( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': payload} )

def deleteKey(ipPort, key, payload):
    #print('DELETE: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.delete( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': payload} )

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

    def checkChangeShardNumber(self, ipPort, expectedStatus, expectedResult, newShardNumber):
        response = changeShardNumber(ipPort, newShardNumber)

        self.assertEqual(response.status_code, expectedStatus)
        data = response.json()
        self.assertEqual(data["result"], expectedResult)
        return data["shard_ids"].split(",")

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

        ipPort = self.view[0]["testScriptAddress"]
        initialShardIDs = self.checkGetAllShardIds(ipPort)

        # remove a node, confirm it's removal
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 2 shards
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # attempt to add shard (should be rejected)
        self.checkChangeShardNumber(ipPort=ipPort,
                               expectedStatus=400,
                               expectedResult="Error",
                               newShardNumber=3)

        # add a node back, confirm its addition
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

        time.sleep(propogationTime)
        
        # check to ensure the shard number hasn't changed
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # attempt to add shard (should be accepted)
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

        ipPort = self.view[0]["testScriptAddress"]
        initialShardIDs = self.checkGetAllShardIds(ipPort)

        # remove a node, confirm it's removal
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 2 shards (five nodes left)
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # remove a node, confirm it's removal
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 2 shards (four nodes left)
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # remove a node, confirm it's removal
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 1 shard (3 nodes left)
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs)-1)

        # remove a node, confirm it's removal
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 1 shard (2 nodes left)
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))

        # remove a node, confirm it's removal
        initialShardIDs = self.checkGetAllShardIds(ipPort)
        removedNode = self.view.pop()["networkIpPortAddress"]
        self.confirmDeleteNode(ipPort=ipPort,
                               removedAddress=removedNode,
                               expectedStatus=200,
                               expectedResult="Success",
                               expectedMsg="Successfully removed %s from view"%removedNode)

        time.sleep(propogationTime)

        # check for 1 shard (1 node left)
        newShardIDs = self.checkGetAllShardIds(ipPort)
        self.assertEqual(len(newShardIDs), len(initialShardIDs))


    def test_a_add_key_value_one_node(self):

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


if __name__ == '__main__':
    unittest.main()
