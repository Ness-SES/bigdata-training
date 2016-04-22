#!/usr/bin/env bash

flume-ng agent -n agent99 --conf conf -f flume_agents.properties -Dflume.root.logger=INFO,console
