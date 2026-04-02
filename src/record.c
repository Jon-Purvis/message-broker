#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "../include/record.h"

/*
 * ----------------------------------------------------------------------------
 * PUBLIC API METHODS
 * ----------------------------------------------------------------------------
 */
int record_init(struct record *record, uint64_t timestamp,
		uint32_t value_length, const void *value)
{
	if (!record || !value) return -1;
	if (value_length == 0) return -1;

	record->header.timestamp = timestamp;
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

ssize_t record_serialize(const struct record *record, void *buf, size_t buf_len)
{
	size_t total_len;
	char *p = (char *)buf;

	if (!record || !record->value || !buf) return -1;

	total_len = sizeof(struct record_header) + record->header.value_length;
	if (buf_len < total_len) return -1;

	memcpy(p, &record->header, sizeof(struct record_header));
	p += sizeof(struct record_header);

	memcpy(p, record->value, record->header.value_length);
	p += record->header.value_length;

	return (ssize_t)(p - (char *)buf);
}

ssize_t record_deserialize(struct record *record, const void *buf, size_t buf_len)
{
	if (!record || !buf || buf_len < sizeof(struct record_header))
		return -1;

	const char *p = (char *)buf;
	memcpy(&record->header, p, sizeof(struct record_header));
	p += sizeof(struct record_header);

	if (buf_len < sizeof(struct record_header) +
			record->header.value_length) return -1;

	record->value = malloc(record->header.value_length);
	if (!record->value) return -1;

	memcpy(record->value, p, record->header.value_length);
	p += record->header.value_length;

	return (ssize_t)(p - (const char *)buf);
}

