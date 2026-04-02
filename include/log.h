#ifndef LOG_H
#define LOG_H

#include "segment.h"
#include "record.h"

#define LOG_EXTENSION ".log"
#define INDEX_EXTENSION ".index"

#define INITIAL_SEGMENT_CAPACITY 8

struct log {
	char *base_path;
	struct segment *active_segment;
	size_t segment_count;
	size_t segment_capacity;
	struct segment **segments;
};

int log_init(struct log *log, const char *base_path);

void log_destroy(struct log *log);

int log_write(struct log *log, struct record *record);

int log_read(struct log *log, struct record *record, uint64_t offset);

#endif

