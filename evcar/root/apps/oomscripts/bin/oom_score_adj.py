#!/usr/bin/python

import os.path
import subprocess
import sys
import urllib2
import json

# get highest memcached score, make other running programs have higher OOM score

# used to read score value from file
def cat(path):
  f = open(path,"r");
  content = f.read();
  return int(content.rstrip());

# get actual unadjusted score
def getScore(pid):
  oomScorePath = "/proc/" + pid + "/oom_score"
  oomAdjPath = "/proc/" + pid + "/oom_score_adj"

  if os.path.isfile(oomScorePath):
    currScore = cat(oomScorePath)
    currAdj = 0
    if os.path.isfile(oomAdjPath):
      currAdj = cat(oomAdjPath)

    currScore = currScore - currAdj

    if currScore > 1:
      return currScore

  return 1;
  
def enabled():
    response = urllib2.urlopen('http://localhost:8078/dynamicproperties?id=evcache.oomscripts.oom_score_adj.enabled')
    if response.getcode() != 200:
        return False
    
    data = json.load(response);
    if data["evcache.oomscripts.oom_score_adj.enabled"] is None or data["evcache.oomscripts.oom_score_adj.enabled"] == "true":
        return True;
    
    return False

def getAdjIncr():
    incr = 10
    response = urllib2.urlopen('http://localhost:8078/dynamicproperties?id=evcache.oomscripts.oom_score_adj.increment')
    if response.getcode() != 200:
        return None
    
    data = json.load(response);
    if data["evcache.oomscripts.oom_score_adj.increment"] is None:
        return incr
    
    try:
        incr = int(data["evcache.oomscripts.oom_score_adj.increment"])
    except ValueError:
        return None
    
    return incr;


# main

# check disabled via local file
if os.path.isfile("/apps/oomscripts/conf/disabled"):
  sys.exit()

# check disabled via fastprop
if not enabled():
  sys.exit()
  
# get adjustment
adjIncr = getAdjIncr()
if adjIncr is None:
    sys.exit()

# get highest memcached score
memcachedScore=1
pidOfVals = subprocess.check_output(["/bin/pidof","memcached"])
for pid in pidOfVals.rstrip().split():
  maxRetVal = getScore(pid)
  if memcachedScore < maxRetVal:
    memcachedScore = maxRetVal


exeList = [ "java", "platformservice.pl", "epic_plugin.pl" ]

# set each running program with OOM score 10 higher than the last one
score = memcachedScore + adjIncr
for exe in exeList:

  retVal = subprocess.check_output(["/bin/pidof","-x",exe])
  for pid in retVal.rstrip().split():
    scoreRetVal = getScore(pid)

    if score >= scoreRetVal:
      adj = score - scoreRetVal;
    else:
      adj = 0;

    subprocess.check_call(["echo " + str(adj) + " > /proc/" + pid + "/oom_score_adj"], shell=True);

  score+=adjIncr


