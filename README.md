# DataChallenge
Build the .jar with following command:

mvn clean compile assembly:single

.jar is generated in target folder, rename it to something clearer (e.g. UniqueUsers.jar).

There are 6 scripts in scripts folder:
- envsetup.sh
- start_zookeeper_kafka.sh
- stop_zookeeper_kafka.sh
- ingest_data.sh
- consume_output.sh
- kill_zookeeper_kafka.sh

Along with scripts, a kafka tarball is included. 

Extract its content to any folder, and copy all the scripts, as well as the .jar file to that folder.

Running the app consists of following operations:
1) Change the path of KAFKA_HOME in envsetup.sh script. Be sure to keep it in the same folder as all other scripts, since the other scripts source envsetup.sh in order to use KAFKA_HOME
2) Run start_zookeeper_kafka.sh
3) Run consume_output.sh in a separate bash window (The script expects kafka server uri and the topic name from which it will consume)
4) Run java app in a separate bash window
	- java -jar UniqueUsers.jar inputTopic outputTopic kafkaServer
	- be sure to supply all 3 arguments
5) Run ingest_data.sh in a separate bash window (The script expects path to file that contains data, kafka server uri and the topic to produce to)

Run stop_zookeeper_kafka.sh in order to stop zookeeper and kafka servers.

Run kill_zookeeper_kafka.sh in order to kill both servers (if anything unexpected is happening).

Zookeeper uses port 2181, while Kafka uses port 9092, so be sure to have them unoccupied before running everything.
