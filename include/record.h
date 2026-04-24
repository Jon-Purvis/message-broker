#ifndef RECORD_H
#define RECORD_H

#include <stdint.h>
#include <unistd.h>

struct record_header {
	uint64_t timestamp;
	uint64_t offset;
	uint32_t crc;
	uint32_t value_length;
};

struct record {
	struct record_header header;
	void *value;
};

int record_init(struct record *record,
				uint64_t timestamp,
				uint32_t value_length,
				const void *value);

void record_destroy(struct record *record);

// serialize into caller-provided buffer, returns bytes written or -1
ssize_t
record_serialize(const struct record *record, void *buf, size_t buf_len);

// deserialize from buffer, returns bytes consumed or -1
ssize_t
record_deserialize(struct record *record, const void *buf, size_t buf_len);

#endif
