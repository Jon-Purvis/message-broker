#ifndef SEGMENT_H
#define SEGMENT_H

#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

#include "record.h"

/* currently hardcoded 64 MB. Pulled from config file later. */
#define SEGMENT_MAX_BYTES (1024 * 1024 * 64)

#define SEGMENT_OK          0
#define SEGMENT_ERR        -1
#define SEGMENT_FULL       -2
#define SEGMENT_NOT_FOUND  -3
#define SEGMENT_IO_ERR     -4

struct index_entry {
	uint64_t offset;
	int64_t position;
};

struct segment {
	size_t segment_size;
	uint64_t base_offset;
	uint64_t current_offset;

	char *log_file_path;
	char *index_file_path;

	int index_fd;
	int log_fd;
};

int segment_init(struct segment *segment, uint64_t base_offset,
		const char *log_file_path, const char *index_file_path);

void segment_destroy(struct segment *segment);

int segment_append(struct segment *segment, const struct record *record);

int segment_read(struct segment *segment, struct record *record, uint64_t offset);

#endif

