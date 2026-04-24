#ifndef TOPIC_H
#define TOPIC_H

#include <stdint.h>
#include "partition.h"

struct topic {
	char *name;
	struct partition **partitions;
	uint32_t partition_count;
};

int topic_init(struct topic *topic,
			   const char *name,
			   const char *base_path,
			   uint32_t partition_count);

void topic_destroy(struct topic *topic);

int topic_write(struct topic *topic,
				const void *key,
				size_t key_len,
				struct record *record,
				uint32_t *assigned_partition_out);

int topic_read(struct topic *topic,
			   struct record *record,
			   uint32_t partition,
			   uint64_t offset);

#endif
