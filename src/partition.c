#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

#include "../include/partition.h"
#include "../include/segment.h"
#include "../include/record.h"
#include "../include/util.h"

static int partition_file_filter(const struct dirent *entry)
{
	const char *extension = strrchr(entry->d_name, '.');
	return (extension && strcmp(extension, LOG_EXTENSION) == 0);
}

static void free_namelist(struct dirent **list, int num_entries)
{
	int entry_index;

	for (entry_index = 0; entry_index < num_entries; entry_index++)
		free(list[entry_index]);
	free(list);
}

static char *build_filename(uint64_t offset, const char *extension)
{
	int len = snprintf(NULL, 0, "%020" PRIu64 "%s", offset, extension);
	if (len < 0)
		return NULL;
	char *result = malloc(len + 1);
	if (result)
		snprintf(result, len + 1, "%020" PRIu64 "%s", offset, extension);
	return result;
}

static struct segment *partition_create_segment(struct partition *partition,
												uint64_t base_offset)
{
	struct segment *segment = NULL;
	char *log_filename = NULL;
	char *index_filename = NULL;
	char *log_file_path = NULL;
	char *index_file_path = NULL;

	log_filename = build_filename(base_offset, LOG_EXTENSION);
	index_filename = build_filename(base_offset, INDEX_EXTENSION);
	if (!log_filename || !index_filename)
		goto cleanup;

	log_file_path = path_join(partition->base_path, log_filename);
	index_file_path = path_join(partition->base_path, index_filename);
	if (!log_file_path || !index_file_path)
		goto cleanup;

	segment = malloc(sizeof(*segment));
	if (!segment)
		goto cleanup;
	if (segment_init(segment, base_offset, log_file_path, index_file_path) !=
		SEGMENT_OK) {
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

static int partition_bootstrap(struct partition *partition)
{
	partition->segments =
		calloc(INITIAL_SEGMENT_CAPACITY, sizeof(struct segment *));
	if (!partition->segments) {
		partition_destroy(partition);
		return -1;
	}

	partition->segment_capacity = INITIAL_SEGMENT_CAPACITY;

	struct segment *seg = partition_create_segment(partition, 0);
	if (!seg) {
		partition_destroy(partition);
		return -1;
	}

	partition->segments[0] = seg;
	partition->active_segment = seg;
	partition->segment_count = 1;
	return 0;
}

static char *change_extension(const char *filename, const char *new_extension)
{
	const char *dot = strrchr(filename, '.');
	if (!dot)
		return NULL;

	int prefix_length = (int)(dot - filename);
	int full_length =
		snprintf(NULL, 0, "%.*s%s", prefix_length, filename, new_extension);

	char *result = malloc(full_length + 1);
	if (result)
		snprintf(result,
				 full_length + 1,
				 "%.*s%s",
				 prefix_length,
				 filename,
				 new_extension);

	return result;
}

static struct segment *partition_build_segment(const char *base_path,
											   const char *file_name)
{
	uint64_t offset = strtoull(file_name, NULL, 10);
	struct segment *segment = NULL;
	char *log_path = NULL;
	char *index_name = NULL;
	char *index_path = NULL;

	log_path = path_join(base_path, file_name);
	index_name = change_extension(file_name, INDEX_EXTENSION);
	if (!log_path || !index_name)
		goto cleanup;

	index_path = path_join(base_path, index_name);
	if (!index_path)
		goto cleanup;

	segment = malloc(sizeof(*segment));
	if (segment &&
		segment_init(segment, offset, log_path, index_path) != SEGMENT_OK) {
		free(segment);
		segment = NULL;
	}

cleanup:
	free(log_path);
	free(index_name);
	free(index_path);
	return segment;
}

static int partition_load_segments(struct partition *partition,
								   struct dirent **namelist,
								   int count)
{
	partition->segment_capacity = (size_t)count + INITIAL_SEGMENT_CAPACITY;
	partition->segments =
		calloc(partition->segment_capacity, sizeof(struct segment *));
	if (!partition->segments)
		return -1;

	for (int i = 0; i < count; i++) {
		partition->segments[i] =
			partition_build_segment(partition->base_path, namelist[i]->d_name);
		if (!partition->segments[i])
			return -1;

		partition->segment_count++;
	}

	partition->active_segment =
		partition->segments[partition->segment_count - 1];
	return 0;
}

static int partition_grow_segments(struct partition *partition)
{
	size_t new_capacity = partition->segment_capacity * 2;
	struct segment **grown =
		realloc(partition->segments, new_capacity * sizeof(struct segment *));
	if (!grown)
		return -1;
	partition->segments = grown;
	partition->segment_capacity = new_capacity;
	return 0;
}

static struct segment *partition_find_segment(struct partition *partition,
											  uint64_t offset)
{
	if (partition->segment_count == 0)
		return NULL;

	int low = 0;
	int high = (int)partition->segment_count - 1;
	while (low < high) {
		int mid = low + (high - low + 1) / 2;
		if (partition->segments[mid]->base_offset <= offset)
			low = mid;
		else
			high = mid - 1;
	}
	return partition->segments[low];
}

int partition_init(struct partition *partition, const char *base_path)
{
	if (!partition || !base_path)
		return -1;
	memset(partition, 0, sizeof(*partition));

	partition->base_path = strdup(base_path);
	if (!partition->base_path)
		return -1;

	struct dirent **namelist = NULL;
	int num_entries =
		scandir(base_path, &namelist, partition_file_filter, alphasort);
	if (num_entries < 0) {
		partition_destroy(partition);
		return -1;
	}

	if (num_entries == 0) {
		free_namelist(namelist, num_entries);
		return partition_bootstrap(partition);
	}

	if (partition_load_segments(partition, namelist, num_entries) != 0) {
		free_namelist(namelist, num_entries);
		partition_destroy(partition);
		return -1;
	}

	free_namelist(namelist, num_entries);
	return 0;
}

void partition_destroy(struct partition *partition)
{
	if (!partition)
		return;

	free(partition->base_path);
	for (size_t i = 0; i < partition->segment_count; i++) {
		segment_destroy(partition->segments[i]);
		free(partition->segments[i]);
	}

	free(partition->segments);
	partition->base_path = NULL;
	partition->active_segment = NULL;
	partition->segments = NULL;
	partition->segment_count = 0;
	partition->segment_capacity = 0;
}

int partition_write(struct partition *partition, struct record *record)
{
	if (!partition || !partition->base_path || !partition->active_segment ||
		!partition->segments || !record || !record->value)
		return -1;

	record->header.offset = partition->active_segment->current_offset;

	int result = segment_append(partition->active_segment, record);
	if (result == SEGMENT_OK)
		return 0;
	if (result != SEGMENT_FULL)
		return -1;

	if (partition->segment_count == partition->segment_capacity) {
		if (partition_grow_segments(partition) != 0)
			return -1;
	}

	uint64_t new_base_offset = partition->active_segment->current_offset;
	struct segment *segment =
		partition_create_segment(partition, new_base_offset);
	if (!segment)
		return -1;

	partition->segments[partition->segment_count] = segment;
	partition->active_segment = segment;
	partition->segment_count++;

	if (segment_append(partition->active_segment, record) != SEGMENT_OK) {
		partition->segment_count--;
		partition->active_segment =
			partition->segments[partition->segment_count - 1];
		partition->segments[partition->segment_count] = NULL;
		unlink(segment->log_file_path);
		unlink(segment->index_file_path);
		segment_destroy(segment);
		free(segment);
		return -1;
	}

	return 0;
}

int partition_write_replica(struct partition *partition, struct record *record)
{
	if (!partition || !partition->active_segment || !record)
		return -1;
	if (record->header.offset != partition->active_segment->current_offset)
		return -1;
	return partition_write(partition, record);
}

int partition_read(struct partition *partition,
				   struct record *record,
				   uint64_t offset)
{
	if (!partition || !partition->base_path || !partition->active_segment ||
		!partition->segments || !record)
		return -1;

	struct segment *target_segment = partition_find_segment(partition, offset);
	if (!target_segment)
		return -1;

	return segment_read(target_segment, record, offset);
}
