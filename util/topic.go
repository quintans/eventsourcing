package util

import (
	"strconv"

	"github.com/quintans/faults"
)

func PartitionTopic(topic string, hash, partitions uint32) (Topic, error) {
	if partitions == 0 {
		return NewPartitionedTopic(topic, 0)
	}

	m := WhichPartition(hash, partitions)
	return NewPartitionedTopic(topic, m)
}

func WhichPartition(hash, partitions uint32) uint32 {
	if partitions <= 1 {
		return 0
	}
	return (hash % partitions) + 1
}

type Topic struct {
	root      string
	partition uint32
}

func NewTopic(root string) (Topic, error) {
	return NewPartitionedTopic(root, 0)
}

func NewPartitionedTopic(root string, partition uint32) (Topic, error) {
	if root == "" {
		return Topic{}, faults.New("topic root cannot be empty")
	}
	return Topic{
		root:      root,
		partition: partition,
	}, nil
}

func (t Topic) String() string {
	if t.partition == 0 {
		return t.root
	}
	return t.root + "#" + strconv.Itoa(int(t.partition))
}

func (t Topic) Root() string {
	return t.root
}

func (t Topic) Partition() uint32 {
	return t.partition
}

func (t Topic) IsZero() bool {
	return t == Topic{}
}
