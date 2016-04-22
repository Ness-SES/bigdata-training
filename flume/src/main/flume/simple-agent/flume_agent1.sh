#!/usr/bin/env bash

flume-ng agent --name agent1 --conf conf --conf-file flume_agent1.properties -Dflume.root.logger=INFO,console
