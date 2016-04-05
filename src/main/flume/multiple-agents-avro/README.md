# Flume advanced agent configuration

Check the `flume_agents.properties` for a more advanced Flume agents configuration. There are three agents defined:
1. A spooling directory agent that monitors a folder for new files and reads them as they appear. It then sinks to a localhost Avro sink that binds to `localhost` on a given port.
2. The 2nd agent uses an Avro source to read from the previous agent. It has defined an interceptor that adds a timestamp to the message header, again sinking to an Avro sink, bound to `localhost`.
3. THe 3rd agent again uses an Avro source to read from he previous agent. It then writes to a given HDFS path.

Some configuration from the file need to be adapted to the environment that's going to be used:
* `agent13.sources.spoolDirSource.spoolDir` - set it to the your source folder where to look for files;
* `agent86.sinks.hdfsSink.hdfs.path` - set it to the HDFS path where final messages are to be written;
* ports can be changed, if the ones in the file are being used.

Now, run in parallel each `flume_agent<n>.sh` file.
