#ifndef NETWORK_H
#define NETWORK_H

#include <stdint.h>
#include <stddef.h>

#define MAX_PACKET_SIZE 4096
#define NETWORK_HEADER_WIRE_SIZE 48

enum command_type {
	CMD_PRODUCE = 1,
	CMD_CONSUME = 2,
	CMD_CREATE_TOPIC = 3,
	CMD_REPLICATE = 4,
};

enum network_io_result {
	NETWORK_IO_PROGRESS = 0,
	NETWORK_IO_COMPLETE = 1,
	NETWORK_IO_WOULD_BLOCK = 2,
	NETWORK_IO_PEER_CLOSED = 3,
	NETWORK_IO_ERROR = 4,
};

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

int message_init(struct message *msg,
				 enum command_type cmd,
				 uint16_t flags,
				 const void *topic,
				 uint32_t topic_length,
				 const void *key,
				 uint32_t key_length,
				 const void *value,
				 uint32_t value_length,
				 uint32_t partition_index,
				 uint64_t consume_offset,
				 uint32_t create_partition_count);

void message_destroy(struct message *message);

/* recomputes header crc from current header fields and bodies (zlib CRC32) */
void message_refresh_crc(struct message *msg);

int network_listen(uint16_t port);

/*
 * blocking TCP client
 */
int network_connect(const char *host, uint16_t port);

int network_accept(int server_fd);

int network_recv_packet(int client_fd, struct message *msg);

int network_send_packet(int client_fd, const struct message *msg);

int network_set_nonblocking(int fd);

enum network_io_result
network_recv_into_buffer_step(int fd,
							  uint8_t *buffer,
							  size_t buffer_length,
							  size_t *bytes_received_in_out);

enum network_io_result network_send_from_buffer_step(int fd,
													 const uint8_t *buffer,
													 size_t buffer_length,
													 size_t *bytes_sent_in_out);

int network_decode_packet_buffers(
	const uint8_t header_buffer[NETWORK_HEADER_WIRE_SIZE],
	const uint8_t *body_buffer,
	size_t body_buffer_length,
	struct message *msg_out);

/*
 * wire response: status (u32 BE), body length (u32 BE), body.
 * CMD_PRODUCE success body: partition_index (u32 BE), record offset (u64 BE).
 */
int network_build_response_buffer(int status_code,
								  const void *payload,
								  size_t payload_length,
								  uint8_t **buffer_out,
								  size_t *buffer_length_out);

void network_response_deinit(struct network_response *resp);

int network_recv_response(int client_fd, struct network_response *resp);

int network_send_response(int client_fd,
						  int status_code,
						  const void *payload,
						  size_t len);

#endif
