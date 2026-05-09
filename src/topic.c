#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

#include "../include/topic.h"
#include "../include/partition.h"
#include "../include/util.h"

static uint32_t topic_partition_index_for_key(const void *key,
											  size_t key_byte_count,
											  uint32_t partition_count)
{
	uint32_t hash;

	if (!key || key_byte_count == 0)
		return 0;

	hash =
		(uint32_t)crc32(0L, (const unsigned char *)key, (uInt)key_byte_count);
	return hash % partition_count;
}

static int ensure_directory_exists(const char *path)
{
	if (mkdir(path, 0755) == -1 && errno != EEXIST)
		return -1;
	return 0;
}

static char *partition_directory_path(const char *topic_path, uint32_t index)
{
	char *part_name;
	char *full_path;
	int name_length;

	name_length = snprintf(NULL, 0, "part-%u", index);
	part_name = malloc((size_t)name_length + 1);
	if (!part_name)
		return NULL;

	snprintf(part_name, (size_t)name_length + 1, "part-%u", index);
	full_path = path_join(topic_path, part_name);
	free(part_name);
	return full_path;
}

static int topic_attach_partition(struct topic *topic,
								  uint32_t index,
								  const char *topic_path)
{
	char *partition_path;
	struct partition *partition;

	partition_path = partition_directory_path(topic_path, index);
	if (!partition_path)
		return -1;

	if (ensure_directory_exists(partition_path) != 0) {
		free(partition_path);
		return -1;
	}

	partition = malloc(sizeof(*partition));
	if (!partition) {
		free(partition_path);
		return -1;
	}

	if (partition_init(partition, partition_path) != 0) {
		free(partition);
		free(partition_path);
		return -1;
	}

	topic->partitions[index] = partition;
	free(partition_path);
	return 0;
}

int topic_init(struct topic *topic,
			   const char *name,
			   const char *base_path,
			   uint32_t partition_count)
{
	char *topic_path;
	uint32_t partition_index;

	if (!topic || !name || !base_path || partition_count == 0)
		return -1;

	memset(topic, 0, sizeof(*topic));
	topic->name = strdup(name);
	topic->partition_count = partition_count;
	topic->partitions = calloc(partition_count, sizeof(struct partition *));

	if (!topic->name || !topic->partitions)
		goto fail;

	topic_path = path_join(base_path, name);
	if (!topic_path)
		goto fail;

	if (ensure_directory_exists(topic_path) != 0) {
		free(topic_path);
		goto fail;
	}

	for (partition_index = 0; partition_index < partition_count;
		 partition_index++) {
		if (topic_attach_partition(topic, partition_index, topic_path) != 0) {
			free(topic_path);
			goto fail;
		}
	}

	free(topic_path);
	return 0;

fail:
	topic_destroy(topic);
	return -1;
}

void topic_destroy(struct topic *topic)
{
	uint32_t partition_index;

	if (!topic)
		return;

	if (topic->partitions) {
		for (partition_index = 0; partition_index < topic->partition_count;
			 partition_index++) {
			if (!topic->partitions[partition_index])
				continue;
			partition_destroy(topic->partitions[partition_index]);
			free(topic->partitions[partition_index]);
		}
		free(topic->partitions);
	}

	free(topic->name);
	topic->name = NULL;
	topic->partitions = NULL;
	topic->partition_count = 0;
}

int topic_write(struct topic *topic,
				const void *key,
				size_t key_len,
				struct record *record,
				uint32_t *assigned_partition_out)
{
	uint32_t partition_index;
	int write_result;

	if (!topic || !record || !topic->partitions)
		return -1;

	partition_index =
		topic_partition_index_for_key(key, key_len, topic->partition_count);
	write_result = partition_write(topic->partitions[partition_index], record);

	if (write_result == 0 && assigned_partition_out)
		*assigned_partition_out = partition_index;
	return write_result;
}

int topic_read(struct topic *topic,
			   struct record *record,
			   uint32_t partition,
			   uint64_t offset)
{
	if (!topic || !topic->partitions || partition >= topic->partition_count)
		return -1;

	return partition_read(topic->partitions[partition], record, offset);
}

int topic_replicate_write(struct topic *topic,
						  uint32_t partition_index,
						  struct record *record)
{
	if (!topic || !topic->partitions ||
		partition_index >= topic->partition_count || !record)
		return -1;

	return partition_write_replica(topic->partitions[partition_index],
								   record);
}
