#ifndef NETWORK_H
#define NETWORK_H

#include <stdint.h>
#include <stddef.h>

#define MAX_PACKET_SIZE 4096

typedef enum {
	CMD_PRODUCE = 1,
	CMD_CONSUME = 2,
	CMD_CREATE_TOPIC = 3,
} command_type_t;

struct message_header {
	uint32_t crc;
	uint32_t total_size;
	uint64_t timestamp;
	uint16_t msg_type;
	uint16_t flags;
	uint32_t topic_length;
	uint32_t key_length;
	uint32_t value_length;
	uint32_t partition_index;
	uint64_t consume_offset;
	/* CMD_CREATE_TOPIC: number of partitions; must be 0 for PRODUCE / CONSUME */
	uint32_t create_partition_count;
};

struct message {
	struct message_header header;
	void *topic;
	void *key;
	void *value;
};

struct network_response {
	int status_code;
	uint8_t *body;
	uint32_t body_length;
};

int message_init(struct message *msg, command_type_t cmd, uint16_t flags,
        const void *topic, uint32_t topic_length,
        const void *key, uint32_t key_length,
        const void *value, uint32_t value_length,
        uint32_t partition_index, uint64_t consume_offset,
        uint32_t create_partition_count);

void message_destroy(struct message *message);

/* Recomputes header crc from current header fields and bodies (zlib CRC32). */
void message_refresh_crc(struct message *msg);

int network_listen(uint16_t port);

int network_accept(int server_fd);

int network_recv_packet(int client_fd, struct message *msg);

int network_send_packet(int client_fd, const struct message *msg);

void network_response_deinit(struct network_response *resp);

int network_recv_response(int client_fd, struct network_response *resp);

int network_send_response(int client_fd, int status_code,
		const void *payload, size_t len);

#endif

