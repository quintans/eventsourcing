package common

import (
	"strconv"
)

func TopicWithPartition(topic string, partition uint32) string {
	if partition == 0 {
		return topic
	}
	return topic + "." + strconv.Itoa(int(partition))
}

func PartitionTopic(key, topic string, partitions uint32) string {
	if partitions == 0 {
		return topic
	}

	m := WhichPartition(key, partitions)
	return TopicWithPartition(topic, m)
}

func WhichPartition(s string, partitions uint32) uint32 {
	if partitions == 0 {
		return 0
	}
	hash := Hash(s)
	return (hash % partitions) + 1
}
