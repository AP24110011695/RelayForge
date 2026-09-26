package broker

// OverviewSnapshot renders the broker's state in plain language for the
// Overview page. It reuses data the broker already computes — topic
// partitions with their log ends (events stored), consumer-group
// membership, and committed offsets versus the log end (lag) — and adds
// no new state.

import (
	"sort"
	"time"

	"relayforge/internal/observability"
)

func (b *Broker) OverviewSnapshot() observability.OverviewData {
	data := observability.OverviewData{GeneratedAt: time.Now()}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		data.Healthy = false
		data.Replication = replicationSentence(b.isFollower)
		return data
	}
	type partRef struct {
		id int32
		hw func() uint64
	}
	perTopic := map[string][]partRef{}
	for topic, parts := range b.parts {
		for _, p := range parts {
			ref := partRef{id: p.id}
			log := p.log
			ref.hw = log.NextOffset
			perTopic[topic] = append(perTopic[topic], ref)
		}
	}
	isFollower := b.isFollower
	b.mu.RUnlock()
	data.Healthy = true
	data.Replication = replicationSentence(isFollower)

	topics := make([]string, 0, len(perTopic))
	for topic := range perTopic {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		refs := perTopic[topic]
		var events uint64
		for _, ref := range refs {
			events += ref.hw()
		}
		data.Streams = append(data.Streams, observability.StreamRow{
			Topic: topic, Partitions: len(refs), Events: events,
		})
	}

	for _, group := range b.groups.Groups() {
		_, members, err := b.groups.Describe(group)
		if err != nil || len(members) == 0 {
			continue
		}
		row := observability.GroupRow{Group: group, Members: len(members)}
		topic := members[0].Topic
		committed, ok := b.groups.CommittedAll(group)
		if ok && len(committed) > 0 {
			row.Started = true
			if refs, known := perTopic[topic]; known {
				for _, ref := range refs {
					if c, has := committed[ref.id]; has {
						if hw := ref.hw(); hw > c {
							row.Lag += hw - c
						}
					}
				}
			}
			row.Behind = row.Lag > 0
		}
		data.Groups = append(data.Groups, row)
	}
	return data
}

func replicationSentence(follower bool) string {
	if follower {
		return "Follower mode: this broker copies from its leader — send events to the leader."
	}
	return "Single broker (standalone)."
}
