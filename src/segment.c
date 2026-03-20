#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include <fcntl.h>
#include <unistd.h>

#include "../include/segment.h"
#include "../include/record.h"

static int segment_write_index(struct segment *segment, off_t physical_position);

static int write_all(int fd, void *buf, size_t buf_len);

int segment_init(struct segment *segment, uint64_t base_offset,
		const char *log_file_path, const char *index_file_path)
{
	if (segment == NULL) return SEGMENT_ERR;

	segment->log_file_path = NULL;
	segment->index_file_path = NULL;
	segment->log_fd = -1;
	segment->index_fd = -1;

	segment->segment_size = 0;
	segment->base_offset = base_offset;
	segment->current_offset = base_offset;

	segment->log_file_path = strdup(log_file_path);
	if (segment->log_file_path == NULL) goto fail;
	segment->index_file_path = strdup(index_file_path);
	if (segment->index_file_path == NULL) goto fail;

	segment->log_fd = open(segment->log_file_path,
			O_CREAT | O_APPEND | O_RDWR,
			S_IRUSR | S_IWUSR);
	if (segment->log_fd == -1) goto fail;
	segment->index_fd = open(segment->index_file_path,
			O_CREAT | O_RDWR,
			S_IRUSR | S_IWUSR);
	if (segment->index_fd == -1) goto fail;

	return SEGMENT_OK;
fail:
	segment_destroy(segment);
	return SEGMENT_ERR;
}

void segment_destroy(struct segment *segment)
{
	if (segment == NULL) return;
	if (segment->log_fd != -1) close(segment->log_fd);
	if (segment->index_fd != -1) close(segment->index_fd);
	free(segment->log_file_path);
	free(segment->index_file_path);
}

int segment_append(struct segment *segment, const struct record *record)
{
	if (segment == NULL) return SEGMENT_ERR;
	if (record == NULL) return SEGMENT_ERR;
	if (record->value == NULL) return SEGMENT_ERR;

	if (segment->segment_size >= SEGMENT_MAX_BYTES) return SEGMENT_FULL;

	size_t buf_len = sizeof(struct record_header) + record->header.value_length;
	void *buf = malloc(buf_len);
	if (buf == NULL) return SEGMENT_ERR;

	ssize_t bytes_written = record_serialize(record, buf, buf_len);
	if (bytes_written == -1) {
		free(buf);
		return SEGMENT_ERR;
	}

	off_t physical_position = (off_t)segment->segment_size;

	if (write_all(segment->log_fd, buf, buf_len) == SEGMENT_ERR) {
		free(buf);
		return SEGMENT_ERR;

	}

	/* possible issue here: if this index write fails, then
	 * the log will a record that will be impossible to read becuase
	 * it won't be recorded in the index
	 */
	if (segment_write_index(segment, physical_position) == SEGMENT_ERR) {
		free(buf);
		return SEGMENT_ERR;
	}

	segment->segment_size += buf_len;
	segment->current_offset += 1;

	free(buf);
	return SEGMENT_OK;
}

static int segment_write_index(struct segment *segment, off_t physical_position)
{
	size_t buf_len = sizeof(uint64_t) + sizeof(physical_position);
	void *buf = malloc(buf_len);
	if (buf == NULL) return SEGMENT_ERR;

	char *p = buf;
	memcpy(p, &segment->current_offset, sizeof(uint64_t));
	p += sizeof(uint64_t);
	memcpy(p, &physical_position, sizeof(physical_position));
	p += sizeof(physical_position);

	if (write_all(segment->index_fd, buf, buf_len) == SEGMENT_ERR) {
		free(buf);
		return SEGMENT_ERR;
	}

	free(buf);
	return SEGMENT_OK;
}

static int write_all(int fd, void *buf, size_t buf_len)
{
	size_t total_written = 0;
	while (total_written < buf_len) {
		ssize_t bytes_written = write(fd,
				(char *)buf + total_written,
				buf_len - total_written);
		if (bytes_written == -1) {
			return SEGMENT_ERR;
		}
		total_written += bytes_written;
	}
	return SEGMENT_OK;
}

int segment_read(struct segment *segment, struct record *record, uint64_t offset)
{

}


