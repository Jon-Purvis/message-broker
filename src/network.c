#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
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
static uint32_t request_payload_crc32(const uint8_t *header_wire,
									  const struct message *msg)
{
	uLong crc = crc32(0L, Z_NULL, 0);
	crc = crc32(
		crc, header_wire + 4, (uInt)(NETWORK_HEADER_WIRE_SIZE - 4));
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

static void pack_wire_header(uint8_t hdr[NETWORK_HEADER_WIRE_SIZE],
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

static void unpack_wire_header(
	const uint8_t hdr[NETWORK_HEADER_WIRE_SIZE],
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
	uint8_t hdr[NETWORK_HEADER_WIRE_SIZE];

	if (!msg)
		return;
	pack_wire_header(hdr, msg, 0);
	msg->header.crc = request_payload_crc32(hdr, msg);
}

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
		NETWORK_HEADER_WIRE_SIZE + topic_length + key_length + value_length;
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

	if (client_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK &&
		errno != EINTR)
		perror("accept");
	return client_fd;
}

int network_recv_packet(int client_fd, struct message *msg)
{
	uint8_t header_buf[NETWORK_HEADER_WIRE_SIZE];

	if (!msg)
		return -1;
	memset(msg, 0, sizeof(*msg));

	if (recv_all(client_fd, header_buf, NETWORK_HEADER_WIRE_SIZE) != 0)
		return -1;

	unpack_wire_header(header_buf, msg);

	uint32_t expected_body = NETWORK_HEADER_WIRE_SIZE +
		msg->header.topic_length +
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
	uint8_t hdr[NETWORK_HEADER_WIRE_SIZE];
	uint32_t expected_size;

	if (!msg)
		return -1;

	expected_size = NETWORK_HEADER_WIRE_SIZE + msg->header.topic_length +
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

int network_set_nonblocking(int fd)
{
	int existing_flags = fcntl(fd, F_GETFL, 0);
	if (existing_flags == -1)
		return -1;
	if ((existing_flags & O_NONBLOCK) != 0)
		return 0;
	if (fcntl(fd, F_SETFL, existing_flags | O_NONBLOCK) == -1)
		return -1;
	return 0;
}

enum network_io_result
network_recv_into_buffer_step(int fd,
							  uint8_t *buffer,
							  size_t buffer_length,
							  size_t *bytes_received_in_out)
{
	if (!buffer || !bytes_received_in_out)
		return NETWORK_IO_ERROR;
	if (*bytes_received_in_out > buffer_length)
		return NETWORK_IO_ERROR;
	if (*bytes_received_in_out == buffer_length)
		return NETWORK_IO_COMPLETE;

	ssize_t bytes_read = recv(fd,
							  buffer + *bytes_received_in_out,
							  buffer_length - *bytes_received_in_out,
							  0);
	if (bytes_read > 0) {
		*bytes_received_in_out += (size_t)bytes_read;
		if (*bytes_received_in_out == buffer_length)
			return NETWORK_IO_COMPLETE;
		return NETWORK_IO_PROGRESS;
	}
	if (bytes_read == 0)
		return NETWORK_IO_PEER_CLOSED;
	if (errno == EAGAIN || errno == EWOULDBLOCK)
		return NETWORK_IO_WOULD_BLOCK;
	if (errno == EINTR)
		return NETWORK_IO_PROGRESS;
	return NETWORK_IO_ERROR;
}

enum network_io_result network_send_from_buffer_step(int fd,
													 const uint8_t *buffer,
													 size_t buffer_length,
													 size_t *bytes_sent_in_out)
{
	if (!buffer || !bytes_sent_in_out)
		return NETWORK_IO_ERROR;
	if (*bytes_sent_in_out > buffer_length)
		return NETWORK_IO_ERROR;
	if (*bytes_sent_in_out == buffer_length)
		return NETWORK_IO_COMPLETE;

	ssize_t bytes_written = send(
		fd, buffer + *bytes_sent_in_out, buffer_length - *bytes_sent_in_out, 0);
	if (bytes_written > 0) {
		*bytes_sent_in_out += (size_t)bytes_written;
		if (*bytes_sent_in_out == buffer_length)
			return NETWORK_IO_COMPLETE;
		return NETWORK_IO_PROGRESS;
	}
	if (bytes_written == 0)
		return NETWORK_IO_PROGRESS;
	if (errno == EAGAIN || errno == EWOULDBLOCK)
		return NETWORK_IO_WOULD_BLOCK;
	if (errno == EINTR)
		return NETWORK_IO_PROGRESS;
	return NETWORK_IO_ERROR;
}

static int network_allocate_and_copy_body_slice(
	const uint8_t *request_body_buffer,
	size_t request_body_length,
	size_t *request_body_offset_in_out,
	uint32_t field_length,
	void **field_out)
{
	if (!request_body_offset_in_out || !field_out)
		return -1;
	*field_out = NULL;
	if (field_length == 0)
		return 0;
	if (!request_body_buffer)
		return -1;
	if (*request_body_offset_in_out > request_body_length)
		return -1;
	if ((size_t)field_length >
		request_body_length - *request_body_offset_in_out) {
		return -1;
	}

	void *allocated_field = malloc(field_length);
	if (!allocated_field)
		return -1;
	memcpy(
		allocated_field, request_body_buffer + *request_body_offset_in_out, field_length);
	*request_body_offset_in_out += field_length;
	*field_out = allocated_field;
	return 0;
}

int network_decode_packet_buffers(
	const uint8_t header_buffer[NETWORK_HEADER_WIRE_SIZE],
	const uint8_t *body_buffer,
	size_t body_buffer_length,
	struct message *msg_out)
{
	uint64_t total_size_wire;
	uint64_t expected_total_size;
	size_t body_offset = 0;

	if (!header_buffer || !msg_out)
		return -1;

	memset(msg_out, 0, sizeof(*msg_out));
	unpack_wire_header(header_buffer, msg_out);

	total_size_wire = (uint64_t)msg_out->header.total_size;
	expected_total_size = (uint64_t)NETWORK_HEADER_WIRE_SIZE +
		(uint64_t)msg_out->header.topic_length +
		(uint64_t)msg_out->header.key_length +
		(uint64_t)msg_out->header.value_length;
	if (expected_total_size != total_size_wire)
		return -1;
	if (body_buffer_length !=
		(size_t)(expected_total_size - NETWORK_HEADER_WIRE_SIZE))
		return -1;
	if (body_buffer_length > 0 && !body_buffer)
		return -1;

	if (network_allocate_and_copy_body_slice(body_buffer,
											 body_buffer_length,
											 &body_offset,
											 msg_out->header.topic_length,
											 &msg_out->topic) != 0) {
		goto fail;
	}
	if (network_allocate_and_copy_body_slice(body_buffer,
											 body_buffer_length,
											 &body_offset,
											 msg_out->header.key_length,
											 &msg_out->key) != 0) {
		goto fail;
	}
	if (network_allocate_and_copy_body_slice(body_buffer,
											 body_buffer_length,
											 &body_offset,
											 msg_out->header.value_length,
											 &msg_out->value) != 0) {
		goto fail;
	}
	if (body_offset != body_buffer_length)
		goto fail;
	if (request_payload_crc32(header_buffer, msg_out) != msg_out->header.crc)
		goto fail;
	return 0;

fail:
	message_destroy(msg_out);
	return -1;
}

int network_build_response_buffer(int status_code,
								  const void *payload,
								  size_t payload_length,
								  uint8_t **buffer_out,
								  size_t *buffer_length_out)
{
	uint8_t *response_buffer = NULL;
	uint8_t *response_cursor = NULL;
	size_t total_length = 8 + payload_length;

	if (!buffer_out || !buffer_length_out)
		return -1;
	*buffer_out = NULL;
	*buffer_length_out = 0;
	if (payload_length > 0 && !payload)
		return -1;

	response_buffer = malloc(total_length);
	if (!response_buffer)
		return -1;
	response_cursor = response_buffer;
	pack_u32(&response_cursor, (uint32_t)status_code);
	pack_u32(&response_cursor, (uint32_t)payload_length);
	if (payload_length > 0)
		memcpy(response_cursor, payload, payload_length);

	*buffer_out = response_buffer;
	*buffer_length_out = total_length;
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
