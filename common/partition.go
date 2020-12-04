package common

import (
	"hash/fnv"
	"strconv"
)

func TopicWithPartition(topic string, partition uint32) string {
	if partition == 0 {
		return topic
	}
	return topic + "." + strconv.Itoa(int(partition))
}

func PartitionTopic(key, topic string, partitions uint32) string {
	if partitions != 0 {
		m := WhichPartition(key, partitions)
		topic = TopicWithPartition(topic, m)
	}
	return topic
}

func WhichPartition(s string, partitions uint32) uint32 {
	hash := Hash(s)
	return (hash % partitions) + 1
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
