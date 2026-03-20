#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../include/record.h"


int record_init(struct record *record, uint64_t timestamp, uint64_t offset,
		uint32_t value_length, const void *value) 
{
	if (record == NULL) return -1;
	if (value == NULL) return -1;
	if (value_length == 0) return -1;

	record->header.timestamp = timestamp;
	record->header.offset = offset;
	record->header.crc = 0;
	record->header.value_length = value_length;

	record->value = malloc(value_length);
	if (record->value == NULL) return -1;

	memcpy(record->value, value, value_length);

	return 0;
}

void record_destroy(struct record *record) 
{
	if (record == NULL) return;
	free(record->value);
	record->value = NULL;
}

// serialize into caller-provided buffer, returns bytes written or -1
int record_serialize(const struct record *record, void *buf, uint32_t buf_len)
{
	if (record == NULL) return -1;
	if (record->value == NULL) return -1;
	if (buf == NULL) return -1;
	if (buf_len == 0) return -1;
	if (buf_len < sizeof(struct record_header) +
			record->header.value_length) return -1;

	char *p = (char *)buf;
	memcpy(p, &record->header, sizeof(struct record_header));
	p += sizeof(struct record_header);
	memcpy(p, record->value, record->header.value_length);
	p += record->header.value_length;

	return p - (char *)buf;
}

// deserialize from buffer, returns bytes consumed or -1
int record_deserialize(struct record *record, const void *buf, uint32_t buf_len)
{
	if (record == NULL) return -1;
	if (buf == NULL) return -1;
	if (buf_len == 0) return -1;
	if (buf_len < sizeof(struct record_header)) return -1;

	char *p = (char *)buf;
	memcpy(&record->header, p, sizeof(struct record_header));
	p += sizeof(struct record_header);

	if (buf_len < sizeof(struct record_header) +
			record->header.value_length) return -1;

	void *caller_record_value = record->value;
	record->value = malloc(record->header.value_length);
	if (record->value == NULL) return -1;
	free(caller_record_value);

	memcpy(record->value, p, record->header.value_length);
	p += record->header.value_length;

	return p - (char *)buf;
}

