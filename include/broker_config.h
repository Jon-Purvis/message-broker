#ifndef BROKER_CONFIG_H
#define BROKER_CONFIG_H

#include <stddef.h>
#include <stdint.h>

#define BROKER_CONFIG_MIN_FRAME_BYTES (4096UL)
#define BROKER_CONFIG_MAX_FRAME_BYTES (64UL * 1024UL * 1024UL)

struct broker_config {
	char *data_dir;
	uint16_t listen_port;
	char *replica_host;
	/* if zero while replica_host is set, the listen port is used */
	uint16_t replica_port;
	size_t max_request_frame_bytes;
	int log_topic_actions;
	int log_client_io;
};

void broker_config_set_defaults(struct broker_config *config);

void broker_config_destroy(struct broker_config *config);

/*
 * returns 0 on success, -1 on malformed line or invalid value, -2 if the file
 * is missing (ENOENT), -3 if the file cannot be opened for another reason.
 */
int broker_config_merge_file(const char *path, struct broker_config *config);

#endif
