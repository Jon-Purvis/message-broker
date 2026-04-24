#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <zlib.h>

#include "../include/network.h"
#include "../include/util.h"

/*
 * Wire: 48-byte big-endian header (see message_header), then topic, key, value.
 * CRC: zlib CRC32 over header[4..48) plus topic, key, value (crc field
 * excluded).
 */
#define HEADER_WIRE_SIZE 48

static uint32_t request_payload_crc32(const uint8_t *header_wire,
									  const struct message *msg)
{
	uLong crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, header_wire + 4, (uInt)(HEADER_WIRE_SIZE - 4));
	if (msg->header.topic_length > 0 && msg->topic)
		crc = crc32(
			crc, (const Bytef *)msg->topic, (uInt)msg->header.topic_length);
	if (msg->header.key_length > 0 && msg->key)
		crc = crc32(crc, (const Bytef *)msg->key, (uInt)msg->header.key_length);
	if (msg->header.value_length > 0 && msg->value)
		crc = crc32(
			crc, (const Bytef *)msg->value, (uInt)msg->header.value_length);
	return (uint32_t)crc;
}

static void pack_wire_header(uint8_t hdr[HEADER_WIRE_SIZE],
							 const struct message *msg,
							 uint32_t crc_field)
{
	uint8_t *p = hdr;

	pack_u32(&p, crc_field);
	pack_u32(&p, msg->header.total_size);
	pack_u64(&p, msg->header.timestamp);
	pack_u16(&p, msg->header.msg_type);
	pack_u16(&p, msg->header.flags);
	pack_u32(&p, msg->header.topic_length);
	pack_u32(&p, msg->header.key_length);
	pack_u32(&p, msg->header.value_length);
	pack_u32(&p, msg->header.partition_index);
	pack_u64(&p, msg->header.consume_offset);
	pack_u32(&p, msg->header.create_partition_count);
}

static void unpack_wire_header(const uint8_t hdr[HEADER_WIRE_SIZE],
							   struct message *msg)
{
	const uint8_t *p = hdr;

	msg->header.crc = unpack_u32(&p);
	msg->header.total_size = unpack_u32(&p);
	msg->header.timestamp = unpack_u64(&p);
	msg->header.msg_type = unpack_u16(&p);
	msg->header.flags = unpack_u16(&p);
	msg->header.topic_length = unpack_u32(&p);
	msg->header.key_length = unpack_u32(&p);
	msg->header.value_length = unpack_u32(&p);
	msg->header.partition_index = unpack_u32(&p);
	msg->header.consume_offset = unpack_u64(&p);
	msg->header.create_partition_count = unpack_u32(&p);
}

static int recv_all(int fd, void *buf, size_t len)
{
	size_t received = 0;

	while (received < len) {
		ssize_t r = recv(fd, (char *)buf + received, len - received, 0);
		if (r <= 0)
			return -1;
		received += (size_t)r;
	}
	return 0;
}

static int send_all(int fd, const void *buf, size_t len)
{
	size_t sent = 0;

	while (sent < len) {
		ssize_t n = send(fd, (const char *)buf + sent, len - sent, 0);
		if (n <= 0)
			return -1;
		sent += (size_t)n;
	}
	return 0;
}

static int recv_payload_blob(int fd, void **out_data, uint32_t byte_count)
{
	if (byte_count == 0) {
		*out_data = NULL;
		return 0;
	}

	void *blob = malloc(byte_count);
	if (!blob)
		return -1;
	if (recv_all(fd, blob, byte_count) != 0) {
		free(blob);
		return -1;
	}
	*out_data = blob;
	return 0;
}

void message_refresh_crc(struct message *msg)
{
	uint8_t hdr[HEADER_WIRE_SIZE];

	if (!msg)
		return;
	pack_wire_header(hdr, msg, 0);
	msg->header.crc = request_payload_crc32(hdr, msg);
}

int message_init(struct message *msg,
				 command_type_t cmd,
				 uint16_t flags,
				 const void *topic,
				 uint32_t topic_length,
				 const void *key,
				 uint32_t key_length,
				 const void *value,
				 uint32_t value_length,
				 uint32_t partition_index,
				 uint64_t consume_offset,
				 uint32_t create_partition_count)
{
	if (!msg)
		return -1;
	memset(msg, 0, sizeof(*msg));

	msg->header.msg_type = (uint16_t)cmd;
	msg->header.flags = flags;
	msg->header.topic_length = topic_length;
	msg->header.key_length = key_length;
	msg->header.value_length = value_length;
	msg->header.partition_index = partition_index;
	msg->header.consume_offset = consume_offset;
	msg->header.create_partition_count = create_partition_count;
	msg->header.total_size =
		HEADER_WIRE_SIZE + topic_length + key_length + value_length;
	msg->header.timestamp = (uint64_t)time(NULL);

	if (topic && topic_length > 0) {
		msg->topic = malloc(topic_length);
		if (!msg->topic)
			goto fail;
		memcpy(msg->topic, topic, topic_length);
	}
	if (key && key_length > 0) {
		msg->key = malloc(key_length);
		if (!msg->key)
			goto fail;
		memcpy(msg->key, key, key_length);
	}
	if (value && value_length > 0) {
		msg->value = malloc(value_length);
		if (!msg->value)
			goto fail;
		memcpy(msg->value, value, value_length);
	}

	message_refresh_crc(msg);
	return 0;

fail:
	message_destroy(msg);
	return -1;
}

void message_destroy(struct message *msg)
{
	if (!msg)
		return;
	free(msg->topic);
	free(msg->key);
	free(msg->value);
	msg->topic = NULL;
	msg->key = NULL;
	msg->value = NULL;
}

int network_listen(uint16_t port)
{
	char port_str[6];
	struct addrinfo hints;
	struct addrinfo *result = NULL;

	snprintf(port_str, sizeof port_str, "%u", port);
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	int rv = getaddrinfo(NULL, port_str, &hints, &result);
	if (rv != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return -1;
	}

	int sockfd =
		socket(result->ai_family, result->ai_socktype, result->ai_protocol);
	if (sockfd == -1) {
		perror("socket");
		freeaddrinfo(result);
		return -1;
	}

	int yes = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
		perror("setsockopt");
		close(sockfd);
		freeaddrinfo(result);
		return -1;
	}
	if (bind(sockfd, result->ai_addr, result->ai_addrlen) == -1) {
		perror("bind");
		close(sockfd);
		freeaddrinfo(result);
		return -1;
	}

	freeaddrinfo(result);

	if (listen(sockfd, 10) == -1) {
		perror("listen");
		close(sockfd);
		return -1;
	}
	return sockfd;
}

int network_accept(int server_fd)
{
	struct sockaddr_storage peer_addr;
	socklen_t addr_size = sizeof peer_addr;
	int client_fd =
		accept(server_fd, (struct sockaddr *)&peer_addr, &addr_size);

	if (client_fd == -1)
		perror("accept");
	return client_fd;
}

int network_recv_packet(int client_fd, struct message *msg)
{
	uint8_t header_buf[HEADER_WIRE_SIZE];

	if (!msg)
		return -1;
	memset(msg, 0, sizeof(*msg));

	if (recv_all(client_fd, header_buf, HEADER_WIRE_SIZE) != 0)
		return -1;

	unpack_wire_header(header_buf, msg);

	uint32_t expected_body = HEADER_WIRE_SIZE + msg->header.topic_length +
		msg->header.key_length + msg->header.value_length;
	if (msg->header.total_size != expected_body)
		return -1;

	if (recv_payload_blob(client_fd, &msg->topic, msg->header.topic_length) !=
		0)
		goto fail;
	if (recv_payload_blob(client_fd, &msg->key, msg->header.key_length) != 0)
		goto fail;
	if (recv_payload_blob(client_fd, &msg->value, msg->header.value_length) !=
		0)
		goto fail;

	if (request_payload_crc32(header_buf, msg) != msg->header.crc) {
		message_destroy(msg);
		return -1;
	}
	return 0;

fail:
	message_destroy(msg);
	return -1;
}

int network_send_packet(int client_fd, const struct message *msg)
{
	uint8_t hdr[HEADER_WIRE_SIZE];
	uint32_t expected_size;

	if (!msg)
		return -1;

	expected_size = HEADER_WIRE_SIZE + msg->header.topic_length +
		msg->header.key_length + msg->header.value_length;
	if (msg->header.total_size != expected_size)
		return -1;

	pack_wire_header(hdr, msg, 0);
	pack_wire_header(hdr, msg, request_payload_crc32(hdr, msg));

	if (send_all(client_fd, hdr, sizeof hdr) != 0)
		return -1;
	if (msg->header.topic_length > 0 && msg->topic) {
		if (send_all(client_fd, msg->topic, msg->header.topic_length) != 0)
			return -1;
	}
	if (msg->header.key_length > 0 && msg->key) {
		if (send_all(client_fd, msg->key, msg->header.key_length) != 0)
			return -1;
	}
	if (msg->header.value_length > 0 && msg->value) {
		if (send_all(client_fd, msg->value, msg->header.value_length) != 0)
			return -1;
	}
	return 0;
}

void network_response_deinit(struct network_response *resp)
{
	if (!resp)
		return;
	free(resp->body);
	resp->body = NULL;
	resp->body_length = 0;
	resp->status_code = 0;
}

int network_recv_response(int client_fd, struct network_response *resp)
{
	uint8_t wire_prefix[8];

	if (!resp)
		return -1;
	memset(resp, 0, sizeof(*resp));

	if (recv_all(client_fd, wire_prefix, sizeof wire_prefix) != 0)
		return -1;

	const uint8_t *p = wire_prefix;
	uint32_t status_wire = unpack_u32(&p);
	uint32_t body_len = unpack_u32(&p);

	resp->status_code = (int)status_wire;
	resp->body_length = body_len;

	if (body_len == 0)
		return 0;

	resp->body = malloc(body_len);
	if (!resp->body)
		return -1;
	if (recv_all(client_fd, resp->body, body_len) != 0) {
		free(resp->body);
		resp->body = NULL;
		return -1;
	}
	return 0;
}

int network_send_response(int client_fd,
						  int status_code,
						  const void *payload,
						  size_t len)
{
	uint8_t prefix[8];
	uint8_t *p = prefix;

	pack_u32(&p, (uint32_t)status_code);
	pack_u32(&p, (uint32_t)len);

	if (send_all(client_fd, prefix, sizeof prefix) != 0)
		return -1;
	if (len > 0 && payload && send_all(client_fd, payload, len) != 0)
		return -1;
	return 0;
}
