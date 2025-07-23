# start kafka
# s1 : zookeeper, s1,s2,s3 : broker
# only start
echo "start s1_zookeeper"
tmux new -s s1_zookeeper -d "ssh s1 '/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties'"
sleep 10
echo "start s1_broker"
tmux new -s s1_broker -d    "ssh s1 '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties'"
echo "start s2_broker"
tmux new -s s2_broker -d    "ssh s2 '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties'"
echo "start s3_broker"
tmux new -s s3_broker -d    "ssh s3 '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties'"


