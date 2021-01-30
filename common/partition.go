package common

import (
	"strconv"
)

func PartitionTopic(topic string, hash, partitions uint32) string {
	if partitions == 0 {
		return topic
	}

	m := WhichPartition(hash, partitions)
	return TopicWithPartition(topic, m)
}

func WhichPartition(hash, partitions uint32) uint32 {
	if partitions <= 1 {
		return 0
	}
	return (hash % partitions) + 1
}

func TopicWithPartition(topic string, partition uint32) string {
	if partition == 0 {
		return topic
	}
	return topic + "." + strconv.Itoa(int(partition))
}
