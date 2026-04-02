#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

#include "../include/log.h"
#include "../include/segment.h"
#include "../include/record.h"

/*
 * ----------------------------------------------------------------------------
 * PRIVATE HELPER METHODS
 * ----------------------------------------------------------------------------
 */
static int log_file_filter(const struct dirent *entry)
{
	const char *extension = strrchr(entry->d_name, '.');
	return (extension && strcmp(extension, LOG_EXTENSION) == 0);
}

static void free_namelist(struct dirent **list, int num_entries)
{
	for (int i = 0; i < num_entries; i++) {
		free(list[i]);
	}
	free(list);
}

static char *build_filename(uint64_t offset, const char *extension)
{
	int len = snprintf(NULL, 0, "%020" PRIu64 "%s", offset, extension);
	if (len < 0) return NULL;
	char *result = malloc(len + 1);
	if (result) snprintf(result, len + 1, "%020" PRIu64 "%s", offset, extension);
	return result;
}

static char *path_join(const char *directory, const char *filename)
{
	int length = snprintf(NULL, 0, "%s/%s", directory, filename);
	if (length < 0) return NULL;

	char *result = malloc(length + 1);
	if (result) snprintf(result, length + 1, "%s/%s", directory, filename);

	return result;
}

static struct segment *log_create_segment(struct log *log, uint64_t base_offset)
{
	struct segment *segment = NULL;
	char *log_filename = NULL;
	char *index_filename = NULL;
	char *log_file_path = NULL;
	char *index_file_path = NULL;

	log_filename = build_filename(base_offset, LOG_EXTENSION);
	index_filename = build_filename(base_offset, INDEX_EXTENSION);
	if (!log_filename || !index_filename) goto cleanup;

	log_file_path = path_join(log->base_path, log_filename);
	index_file_path = path_join(log->base_path, index_filename);
	if (!log_file_path || !index_file_path) goto cleanup;

	segment = malloc(sizeof(*segment));
	if (!segment) goto cleanup;
	if (segment_init(segment, base_offset, log_file_path, index_file_path) != SEGMENT_OK) {
		free(segment);
		segment = NULL;
	}

cleanup:
	free(log_filename);
	free(index_filename);
	free(log_file_path);
	free(index_file_path);
	return segment;
}

static int log_bootstrap(struct log *log)
{
	log->segments = calloc(INITIAL_SEGMENT_CAPACITY, sizeof(struct segment *));
	if (!log->segments) { log_destroy(log); return -1; }

	log->segment_capacity = INITIAL_SEGMENT_CAPACITY;

	struct segment *seg = log_create_segment(log, 0);
	if (!seg) { log_destroy(log); return -1; }

	log->segments[0] = seg;
	log->active_segment = seg;
	log->segment_count = 1;
	return 0;
}

static char *change_extension(const char *filename, const char *new_extension)
{
	const char *dot = strrchr(filename, '.');
	if (!dot) return NULL;

	int prefix_length = (int)(dot - filename);
	int full_length = snprintf(NULL, 0, "%.*s%s", prefix_length, filename, new_extension);

	char *result = malloc(full_length + 1);
	if (result)
		snprintf(result, full_length + 1, "%.*s%s", prefix_length, filename, new_extension);

	return result;
}

static struct segment *log_build_segment(const char *base_path, const char *file_name)
{
	uint64_t offset = strtoull(file_name, NULL, 10);
	struct segment *segment = NULL;
	char *log_path = NULL;
	char *index_name = NULL;
	char *index_path = NULL;

	log_path   = path_join(base_path, file_name);
	index_name = change_extension(file_name, INDEX_EXTENSION);
	if (!log_path || !index_name) goto cleanup;

	index_path = path_join(base_path, index_name);
	if (!index_path) goto cleanup;

	segment = malloc(sizeof(*segment));
	if (segment && segment_init(segment, offset, log_path, index_path) != 0) {
		free(segment);
		segment = NULL;
	}

cleanup:
	free(log_path);
	free(index_name);
	free(index_path);
	return segment;
}

static int log_load_segments(struct log *log, struct dirent **namelist, int count)
{
	log->segment_capacity = (size_t)count + INITIAL_SEGMENT_CAPACITY;
	log->segments = calloc(log->segment_capacity, sizeof(struct segment *));
	if (!log->segments) return -1;

	for (int i = 0; i < count; i++) {
		log->segments[i] = log_build_segment(log->base_path, namelist[i]->d_name);
		if (!log->segments[i]) return -1;

		log->segment_count++;
	}

	log->active_segment = log->segments[log->segment_count - 1];
	return 0;
}

static int log_grow_segments(struct log *log)
{
	size_t new_capacity = log->segment_capacity * 2;
	struct segment **grown = realloc(log->segments,
			new_capacity * sizeof(struct segment *));
	if (!grown) return -1;
	log->segments = grown;
	log->segment_capacity = new_capacity;
	return 0;
}

static struct segment *log_find_segment(struct log *log, uint64_t offset)
{
	if (log->segment_count == 0) return NULL;

	int low = 0;
	int high = (int)log->segment_count - 1;
	while (low < high) {
		int mid = low + (high - low + 1) / 2;
		if (log->segments[mid]->base_offset <= offset)
			low = mid;
		else
			high = mid - 1;
	}
	return log->segments[low];
}

/*
 * ----------------------------------------------------------------------------
 * PUBLIC API METHODS
 * ----------------------------------------------------------------------------
 */
int log_init(struct log *log, const char *base_path)
{
	if (!log || !base_path) return -1;
	memset(log, 0, sizeof(*log));

	log->base_path = strdup(base_path);
	if (!log->base_path) return -1;

	struct dirent **namelist = NULL;
	int num_entries = scandir(base_path, &namelist, log_file_filter, alphasort);
	if (num_entries < 0) { log_destroy(log); return -1; }

	if (num_entries == 0) {
		free_namelist(namelist, num_entries);
		return log_bootstrap(log);
	}

	if (log_load_segments(log, namelist, num_entries) != 0) {
		free_namelist(namelist, num_entries);
		log_destroy(log);
		return -1;
	}

	free_namelist(namelist, num_entries);
	return 0;
}

void log_destroy(struct log *log)
{
	if (!log) return;

	free(log->base_path);
	for (int i = 0; i < log->segment_count; i++) {
		segment_destroy(log->segments[i]);
		free(log->segments[i]);
	}

	free(log->segments);
	log->base_path = NULL;
	log->active_segment = NULL;
	log->segments = NULL;
	log->segment_count = 0;
	log->segment_capacity = 0;
}


int log_write(struct log *log, struct record *record)
{
	if (!log || !log->base_path || !log->active_segment || !log->segments
			|| !record || !record->value)
		return -1;

	record->header.offset = log->active_segment->current_offset;

	int result = segment_append(log->active_segment, record);
	if (result == SEGMENT_OK) return 0;
	if (result != SEGMENT_FULL) return -1;

	if (log->segment_count == log->segment_capacity) {
		if (log_grow_segments(log) != 0) return -1;
	}

	uint64_t new_base_offset = log->active_segment->current_offset;
	struct segment *segment = log_create_segment(log, new_base_offset);
	if (!segment) return -1;

	log->segments[log->segment_count] = segment;
	log->active_segment = segment;
	log->segment_count++;

	if (segment_append(log->active_segment, record) != SEGMENT_OK) {
		log->segment_count--;
		log->active_segment = log->segments[log->segment_count - 1];
		log->segments[log->segment_count] = NULL;
		unlink(segment->log_file_path);
		unlink(segment->index_file_path);
		segment_destroy(segment);
		free(segment);
		return -1;
	}

	return 0;
}

int log_read(struct log *log, struct record *record, uint64_t offset)
{
	if (!log || !log->base_path || !log->active_segment || !log->segments || !record)
		return -1;

	struct segment *target_segment = log_find_segment(log, offset);
	if (!target_segment) return -1;

	if (segment_read(target_segment, record, offset) != SEGMENT_OK)
		return -1;

	return 0;
}
