#!/usr/bin/env bash

flume-ng agent -n agent13 --conf conf -f flume_agents.properties -Dflume.root.logger=INFO,console
