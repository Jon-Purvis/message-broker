#ifndef BROKER_H
#define BROKER_H

#include <signal.h>
#include <stddef.h>
#include <stdint.h>

#include "broker_config.h"
#include "topic.h"
#include "network.h"

#define BROKER_MAX_TOPICS 64
#define BROKER_MAX_CONNECTIONS 128

enum broker_connection_phase {
	BROKER_CONNECTION_PHASE_READ_HEADER = 0,
	BROKER_CONNECTION_PHASE_READ_BODY = 1,
	BROKER_CONNECTION_PHASE_WRITE_RESPONSE = 2,
};

struct broker_connection {
	int is_in_use;
	int fd;
	enum broker_connection_phase phase;
	uint8_t incoming_header_buffer[NETWORK_HEADER_WIRE_SIZE];
	size_t incoming_header_bytes_received;
	uint8_t *incoming_body_buffer;
	size_t incoming_body_length;
	size_t incoming_body_bytes_received;
	uint8_t *outgoing_response_buffer;
	size_t outgoing_response_length;
	size_t outgoing_response_bytes_sent;
};

struct broker {
	struct broker_connection connections[BROKER_MAX_CONNECTIONS];
	struct topic *topics[BROKER_MAX_TOPICS];
	uint32_t topic_count;
	char *data_dir;
	int server_fd;
	uint16_t port;
	volatile sig_atomic_t stop_requested;
	/* Optional one-way replica (leader forwards produces / creates here). */
	char *replica_host;
	uint16_t replica_port;
	size_t max_request_frame_size_bytes;
	int log_topic_actions;
	int log_client_io;
};

int broker_init(struct broker *broker, const struct broker_config *config);

void broker_destroy(struct broker *broker);

int broker_run(struct broker *broker);

int broker_create_topic(struct broker *broker,
						const char *name,
						uint32_t partition_count);

struct topic *broker_find_topic(struct broker *broker,
								const char *target_name);

#endif
