#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <stddef.h>

#include "../include/segment.h"
#include "../include/record.h"

/*
 * ----------------------------------------------------------------------------
 * PRIVATE HELPER METHODS
 * ----------------------------------------------------------------------------
 */
static int write_all(int fd, const void *buf, size_t buf_len)
{
	size_t written = 0;
	ssize_t res;

	while (written < buf_len) {
		res = write(fd, (const char *)buf + written, buf_len - written);
		if (res <= 0) return SEGMENT_IO_ERR;
		written += (size_t)res;
	}
	return SEGMENT_OK;
}

static int segment_write_index(struct segment *segment, off_t physical_position)
{
	struct index_entry entry = {
		.offset = segment->current_offset,
		.position = physical_position
	};

	return write_all(segment->index_fd, &entry, sizeof(entry));
}

static int get_index_entry(struct segment *segment, int64_t index,
		struct index_entry *entry)
{
	ssize_t result;
	size_t entry_size = sizeof(struct index_entry);

	result = pread(segment->index_fd, entry, entry_size, (off_t)(index * entry_size));
	if (result != (ssize_t)entry_size)
		return SEGMENT_IO_ERR;

	return SEGMENT_OK;
}

static int read_from_log(struct segment *segment, struct record *record, off_t position)
{
	ssize_t result = pread(segment->log_fd, &record->header, sizeof(record->header), position);
	if (result != sizeof(record->header))
		return SEGMENT_IO_ERR;

	record->value = malloc(record->header.value_length);
	if (!record->value)
		return SEGMENT_ERR;

	off_t value_position = position + (off_t)sizeof(struct record_header);
	result = pread(segment->log_fd, record->value, record->header.value_length, value_position);
	if (result != (ssize_t)record->header.value_length) {
		free(record->value);
		record->value = NULL;
		return SEGMENT_IO_ERR;
	}

	return SEGMENT_OK;
}

static int segment_find_position(struct segment *segment, uint64_t target,
		off_t *out_position)
{
	size_t total = segment->current_offset - segment->base_offset;
	int64_t low = 0;
	int64_t high = (int64_t)total - 1;

	while (low <= high) {
		/* safe midpoint calculation that prevents possible overflow */
		int64_t mid = low + (high - low) / 2;
		struct index_entry entry;

		if (get_index_entry(segment, mid, &entry) != SEGMENT_OK)
			return SEGMENT_IO_ERR;

		if (target > entry.offset)
			low = mid + 1;
		else if (target < entry.offset)
			high = mid - 1;
		else {
			*out_position = entry.position;
			return SEGMENT_OK;
		}
	}
	return SEGMENT_NOT_FOUND;
}

/*
 * ----------------------------------------------------------------------------
 * PUBLIC API METHODS
 * ----------------------------------------------------------------------------
 */
int segment_init(struct segment *segment, uint64_t base_offset,
		const char *log_path, const char *index_path)
{
	if (!segment)
		return SEGMENT_ERR;

	segment->log_file_path = NULL;
	segment->index_file_path = NULL;
	segment->log_fd = -1;
	segment->index_fd = -1;

	segment->segment_size = 0;
	segment->base_offset = base_offset;
	segment->current_offset = base_offset;

	segment->log_file_path = strdup(log_path);
	segment->index_file_path = strdup(index_path);

	if (!segment->log_file_path || !segment->index_file_path)
		goto fail;

	segment->log_fd = open(segment->log_file_path,
			O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);
	segment->index_fd = open(segment->index_file_path,
			O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

	if (segment->log_fd == -1 || segment->index_fd == -1)
		goto fail;

	return SEGMENT_OK;

fail:
	segment_destroy(segment);
	return SEGMENT_ERR;
}

void segment_destroy(struct segment *segment)
{
	if (!segment)
		return;

	if (segment->log_fd != -1) close(segment->log_fd);
	if (segment->index_fd != -1) close(segment->index_fd);

	free(segment->log_file_path);
	free(segment->index_file_path);

	segment->log_fd = -1;
	segment->index_fd = -1;
	segment->log_file_path = NULL;
	segment->index_file_path = NULL;
}

int segment_append(struct segment *segment, const struct record *record)
{
	if (!segment || !record || !record->value)
		return SEGMENT_ERR;

	if (segment->segment_size >= SEGMENT_MAX_BYTES)
		return SEGMENT_FULL;

	size_t len = sizeof(struct record_header) + record->header.value_length;
	void *buf = malloc(len);
	if (!buf)
		return SEGMENT_ERR;

	if (record_serialize(record, buf, len) == -1) {
		free(buf);
		return SEGMENT_ERR;
	}

	off_t physical_position = (off_t)segment->segment_size;

	if (write_all(segment->log_fd, buf, len) != SEGMENT_OK) {
		free(buf);
		return SEGMENT_IO_ERR;
	}

	if (segment_write_index(segment, physical_position) != SEGMENT_OK) {
		free(buf);
		return SEGMENT_IO_ERR;
	}

	segment->segment_size += len;
	segment->current_offset += 1;

	free(buf);
	return SEGMENT_OK;
}

int segment_read(struct segment *segment, struct record *record,
		uint64_t target_offset)
{
	off_t position;
	int result;

	if (!segment || !record)
		return SEGMENT_ERR;

	result = segment_find_position(segment, target_offset, &position);
	if (result != SEGMENT_OK)
		return result;

	return read_from_log(segment, record, position);
}

