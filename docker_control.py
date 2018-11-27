# docker_control.py
# Docker Controls from Python
# Mostly called from hw3_test.py to start containers
# can be used from command line (largely for testing specific executions by hand)


import subprocess
import os
import sys
import time
import argparse

class docker_controller:
    sudo = []
    spinUpTime = 5

    def __init__(self, needSudo=False):
        if needSudo:
            self.sudo = ["sudo"]

    def buildDockerImage(self, tag):
        subprocess.run(self.sudo+["docker", "build", "-t", tag, "."])

    def spinUpDockerContainerNoWait(self, tag, hostIp, port, view):
        instance = {"testScriptAddress":hostIp+":"+port}

        # instance["containerID"] = subprocess.getoutput(self.sudo+["docker", "run",
        #                 "-p", "%s:8080"%port,
        #                 "-e", "VIEW=%s"%view,
        #                 "-e", "IP_PORT=%s:%s"%(hostIp, port),
        #                 "-d", tag])
        instance["containerID"] = subprocess.getoutput(
            "docker run -p %s:8080 -e VIEW=%s -e IP_PORT=%s:%s -d %s" % (port, view, hostIp, port, tag))
        print(instance)
        # print("CONTAINER:ID: " + instance["containerID"])

        return instance

    def spinUpDockerContainer(self, tag, hostIp, port, view):

        instance = self.spinUpDockerContainerNoWait(tag, hostIp, port, view)

        time.sleep(self.spinUpTime)

        #print("completed spinning docker container %s"%instance)

        return instance

    def spinUpManyContainers(self, tag, host_ip, port_prefix, number):
        view = []
        for i in range(2, 2+number):
            view.append( "%s:%s%s"%(host_ip, port_prefix,i) )

        viewString = ",".join(view)
        view = []
        for i in range(2, 2+number):
            view.append(self.spinUpDockerContainerNoWait(tag, host_ip, port_prefix+str(i), viewString))

        time.sleep(self.spinUpTime)

        #print("completed spinning docker containers")

        return view

    def cleanUpDockerContainer(self, instance=None):
        if instance == None:
            instance = subprocess.getoutput("docker ps -q")

            instance = instance.split("\n")

            for inst in instance:
                subprocess.run(self.sudo+["docker", "kill", inst])

        else:
            subprocess.run(self.sudo+["docker", "kill", instance])

if __name__ == '__main__':
    #change these variables to change default values
    standardPort = "8080"
    standardIP   = "169.233.245.206"
    standardBuildTag = "testing"

    parser = argparse.ArgumentParser(description='docker controller')
    parser.add_argument('-K', dest="is_kill_mode", action="store_true", help="kill all docker containers that are running")
    parser.add_argument('-B', dest="is_build_mode", action="store_true", help="build a docker image")
    parser.add_argument('-S', dest="is_start_mode", action="store_true", help="start up a docker container")
    parser.add_argument('-t', dest="buildTag", default=standardBuildTag, help="set the build tag. If unset, tag will be: %s"%standardBuildTag)
    parser.add_argument('-port', dest="localPort", default=standardPort, help="set the port to start your container on. Only used with -S. If unset will be: %s"%standardPort)
    parser.add_argument('-hostIp', dest="hostIp", default=standardIP, help="set the ip to start your container on. Only used with -S. If unset will be: %s"%standardIP)

    args = parser.parse_args()

    kill = args.is_kill_mode
    build = args.is_build_mode
    start = args.is_start_mode

    dockerBuildTag = args.buildTag
    localPort = args.localPort
    hostIp = args.hostIp

    dc = docker_controller()

    if kill:
        dc.cleanUpDockerContainer()
    if build:
        dc.buildDockerImage(dockerBuildTag)
    if start:
        dc.spinUpDockerContainer(dockerBuildTag, hostIp, localPort, "%s:8082,%s:8083"%(hostIp, hostIp))
