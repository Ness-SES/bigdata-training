#!/usr/bin/env bash

flume-ng agent -n agent86 --conf conf -f flume_agents.properties -Dflume.root.logger=INFO,console
