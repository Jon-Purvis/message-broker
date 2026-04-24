#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#include "../include/record.h"
#include "../include/util.h"

/*
 * ----------------------------------------------------------------------------
 * PUBLIC API METHODS
 * ----------------------------------------------------------------------------
 */
int record_init(struct record *record,
				uint64_t timestamp,
				uint32_t value_length,
				const void *value)
{
	if (!record || !value)
		return -1;
	if (value_length == 0)
		return -1;

	record->header.timestamp = timestamp;
	record->header.crc =
		(uint32_t)crc32(0L, (const unsigned char *)value, value_length);
	record->header.value_length = value_length;

	record->value = malloc(value_length);
	if (record->value == NULL)
		return -1;

	memcpy(record->value, value, value_length);

	return 0;
}

void record_destroy(struct record *record)
{
	if (record == NULL)
		return;
	free(record->value);
	record->value = NULL;
}

ssize_t record_serialize(const struct record *record, void *buf, size_t buf_len)
{
	if (!record || !record->value || !buf)
		return -1;

	size_t total_len =
		sizeof(struct record_header) + record->header.value_length;
	if (buf_len < total_len)
		return -1;

	uint8_t *p = (uint8_t *)buf;

	pack_u64(&p, record->header.timestamp);
	pack_u64(&p, record->header.offset);
	pack_u32(&p, record->header.crc);
	pack_u32(&p, record->header.value_length);

	memcpy(p, record->value, record->header.value_length);
	p += record->header.value_length;

	return (ssize_t)(p - (uint8_t *)buf);
}

ssize_t
record_deserialize(struct record *record, const void *buf, size_t buf_len)
{
	if (!record || !buf || buf_len < sizeof(struct record_header))
		return -1;

	const uint8_t *p = (const uint8_t *)buf;

	record->header.timestamp = unpack_u64(&p);
	record->header.offset = unpack_u64(&p);
	record->header.crc = unpack_u32(&p);
	record->header.value_length = unpack_u32(&p);

	if (buf_len < sizeof(struct record_header) + record->header.value_length)
		return -1;

	record->value = malloc(record->header.value_length);
	if (!record->value)
		return -1;

	memcpy(record->value, p, record->header.value_length);

	uint32_t expected_crc =
		(uint32_t)crc32(0L, record->value, record->header.value_length);
	if (expected_crc != record->header.crc) {
		free(record->value);
		return -1;
	}

	return (ssize_t)(sizeof(struct record_header) +
					 record->header.value_length);
}
