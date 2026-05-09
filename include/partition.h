#ifndef PARTITION_H
#define PARTITION_H

#include "segment.h"
#include "record.h"

#define LOG_EXTENSION ".log"
#define INDEX_EXTENSION ".index"

#define INITIAL_SEGMENT_CAPACITY 8

struct partition {
	char *base_path;
	struct segment *active_segment;
	size_t segment_count;
	size_t segment_capacity;
	struct segment **segments;
};

int partition_init(struct partition *partition, const char *base_path);

void partition_destroy(struct partition *partition);

int partition_write(struct partition *partition, struct record *record);

int partition_write_replica(struct partition *partition, struct record *record);

int partition_read(struct partition *partition,
				   struct record *record,
				   uint64_t offset);

#endif
