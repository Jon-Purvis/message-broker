#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/broker.h"
#include "../include/network.h"
#include "../include/record.h"
#include "../include/util.h"

static volatile sig_atomic_t *global_stop_flag = NULL;

static void broker_log_topic_action(struct broker *broker,
									const char *action,
									const struct message *message,
									const char *suffix)
{
	char topic_name[128];
	size_t copy_len;

	if (!broker || !broker->log_topic_actions)
		return;
	if (!action || !message || !message->topic ||
		message->header.topic_length == 0)
		return;
	copy_len = message->header.topic_length;
	if (copy_len >= sizeof(topic_name))
		copy_len = sizeof(topic_name) - 1;
	memcpy(topic_name, message->topic, copy_len);
	topic_name[copy_len] = '\0';
	fprintf(stderr,
			"[broker] %s topic=%s%s%s\n",
			action,
			topic_name,
			suffix ? " " : "",
			suffix ? suffix : "");
}

static void broker_connection_reset_state(struct broker_connection *connection)
{
	if (!connection)
		return;
	if (connection->fd >= 0) {
		close(connection->fd);
		connection->fd = -1;
	}
	free(connection->incoming_body_buffer);
	connection->incoming_body_buffer = NULL;
	free(connection->outgoing_response_buffer);
	connection->outgoing_response_buffer = NULL;
	connection->is_in_use = 0;
	connection->phase = BROKER_CONNECTION_PHASE_READ_HEADER;
	connection->incoming_body_length = 0;
	connection->incoming_header_bytes_received = 0;
	connection->incoming_body_bytes_received = 0;
	connection->outgoing_response_length = 0;
	connection->outgoing_response_bytes_sent = 0;
	memset(connection->incoming_header_buffer,
		   0,
		   sizeof(connection->incoming_header_buffer));
}

static void broker_connection_table_init(struct broker *broker)
{
	if (!broker)
		return;
	for (size_t connection_index = 0; connection_index < BROKER_MAX_CONNECTIONS;
		 connection_index++) {
		struct broker_connection *connection =
			&broker->connections[connection_index];
		memset(connection, 0, sizeof(*connection));
		connection->fd = -1;
		connection->phase = BROKER_CONNECTION_PHASE_READ_HEADER;
	}
}

static struct broker_connection *
broker_connection_acquire_free_slot(struct broker *broker)
{
	if (!broker)
		return NULL;
	for (size_t connection_index = 0; connection_index < BROKER_MAX_CONNECTIONS;
		 connection_index++) {
		struct broker_connection *connection =
			&broker->connections[connection_index];
		if (connection->is_in_use)
			continue;
		connection->is_in_use = 1;
		connection->fd = -1;
		connection->phase = BROKER_CONNECTION_PHASE_READ_HEADER;
		connection->incoming_header_bytes_received = 0;
		connection->incoming_body_bytes_received = 0;
		connection->incoming_body_length = 0;
		connection->outgoing_response_length = 0;
		connection->outgoing_response_bytes_sent = 0;
		return connection;
	}
	return NULL;
}

static void
broker_connection_prepare_for_next_request(struct broker_connection *connection)
{
	if (!connection)
		return;
	free(connection->incoming_body_buffer);
	connection->incoming_body_buffer = NULL;
	free(connection->outgoing_response_buffer);
	connection->outgoing_response_buffer = NULL;
	connection->phase = BROKER_CONNECTION_PHASE_READ_HEADER;
	connection->incoming_header_bytes_received = 0;
	connection->incoming_body_bytes_received = 0;
	connection->incoming_body_length = 0;
	connection->outgoing_response_length = 0;
	connection->outgoing_response_bytes_sent = 0;
	memset(connection->incoming_header_buffer,
		   0,
		   sizeof(connection->incoming_header_buffer));
}

static int broker_parse_expected_body_length(
	struct broker *broker,
	const uint8_t header_buffer[NETWORK_HEADER_WIRE_SIZE],
	size_t *expected_body_length_out)
{
	const uint8_t *cursor = header_buffer;
	uint32_t ignored_crc = unpack_u32(&cursor);
	uint32_t total_size = unpack_u32(&cursor);
	uint64_t ignored_timestamp = unpack_u64(&cursor);
	uint16_t ignored_msg_type = unpack_u16(&cursor);
	uint16_t ignored_flags = unpack_u16(&cursor);
	uint32_t topic_length = unpack_u32(&cursor);
	uint32_t key_length = unpack_u32(&cursor);
	uint32_t value_length = unpack_u32(&cursor);
	size_t expected_body_length;
	size_t expected_total_size;

	(void)ignored_crc;
	(void)ignored_timestamp;
	(void)ignored_msg_type;
	(void)ignored_flags;

	if (!broker || !expected_body_length_out)
		return -1;

	expected_body_length =
		(size_t)topic_length + (size_t)key_length + (size_t)value_length;
	expected_total_size = NETWORK_HEADER_WIRE_SIZE + expected_body_length;
	if (expected_total_size != (size_t)total_size)
		return -1;
	if (expected_total_size > broker->max_request_frame_size_bytes)
		return -1;

	*expected_body_length_out = expected_body_length;
	return 0;
}

static void handle_sigint(int signo)
{
	(void)signo;
	if (global_stop_flag)
		*global_stop_flag = 1;
}

static int copy_topic_name_from_message(const struct message *message,
										char **topic_name_out)
{
	if (!message || !topic_name_out)
		return -1;
	if (message->header.topic_length == 0 || !message->topic)
		return -1;

	size_t topic_length = message->header.topic_length;
	char *topic_name = malloc(topic_length + 1);
	if (!topic_name)
		return -1;

	memcpy(topic_name, message->topic, topic_length);
	topic_name[topic_length] = '\0';
	*topic_name_out = topic_name;
	return 0;
}

static int find_topic_from_message(struct broker *broker,
								   const struct message *message,
								   struct topic **topic_out)
{
	if (!broker || !message || !topic_out)
		return -1;

	char *topic_name = NULL;
	if (copy_topic_name_from_message(message, &topic_name) != 0)
		return -1;

	struct topic *topic = broker_find_topic(broker, topic_name);
	free(topic_name);
	if (!topic)
		return -1;

	*topic_out = topic;
	return 0;
}

static void broker_forward_replicate_produce(struct broker *broker,
											 struct topic *topic,
											 uint32_t partition_index,
											 const struct record *record,
											 const struct message *message)
{
	struct message msg;
	size_t topic_len;

	if (broker->replica_host == NULL || topic == NULL || record == NULL ||
		message == NULL)
		return;

	topic_len = strlen(topic->name);
	if (message_init(&msg,
					 CMD_REPLICATE,
					 0,
					 topic->name,
					 (uint32_t)topic_len,
					 message->key,
					 message->header.key_length,
					 record->value,
					 record->header.value_length,
					 partition_index,
					 record->header.offset,
					 0) != 0)
		return;

	msg.header.timestamp = record->header.timestamp;
	message_refresh_crc(&msg);

	int fd = network_connect(broker->replica_host, broker->replica_port);
	if (fd < 0) {
		fprintf(stderr,
				"replica: produce forward connect %s:%u failed\n",
				broker->replica_host,
				(unsigned)broker->replica_port);
		message_destroy(&msg);
		return;
	}

	if (network_send_packet(fd, &msg) != 0) {
		fprintf(stderr, "replica: produce forward send failed\n");
		message_destroy(&msg);
		close(fd);
		return;
	}
	message_destroy(&msg);

	struct network_response resp;
	if (network_recv_response(fd, &resp) != 0) {
		fprintf(stderr, "replica: produce forward recv failed\n");
		close(fd);
		return;
	}
	if (resp.status_code != 0)
		fprintf(
			stderr, "replica: produce follower status=%d\n", resp.status_code);
	else if (broker->log_topic_actions != 0)
		fprintf(
			stderr,
			"[broker] replica_ack produce topic=%s partition=%u offset=%" PRIu64
			"\n",
			topic->name,
			(unsigned)partition_index,
			(uint64_t)record->header.offset);
	network_response_deinit(&resp);
	close(fd);
}

static void broker_forward_replicate_create_topic(struct broker *broker,
												  const struct message *src)
{
	struct message msg;

	if (broker->replica_host == NULL || src == NULL)
		return;

	if (message_init(&msg,
					 CMD_CREATE_TOPIC,
					 0,
					 src->topic,
					 src->header.topic_length,
					 NULL,
					 0,
					 NULL,
					 0,
					 0,
					 0,
					 src->header.create_partition_count) != 0)
		return;

	msg.header.timestamp = src->header.timestamp;
	message_refresh_crc(&msg);

	int fd = network_connect(broker->replica_host, broker->replica_port);
	if (fd < 0) {
		fprintf(stderr,
				"replica: create_topic forward connect %s:%u failed\n",
				broker->replica_host,
				(unsigned)broker->replica_port);
		message_destroy(&msg);
		return;
	}

	if (network_send_packet(fd, &msg) != 0) {
		fprintf(stderr, "replica: create_topic forward send failed\n");
		message_destroy(&msg);
		close(fd);
		return;
	}
	message_destroy(&msg);

	struct network_response resp;
	if (network_recv_response(fd, &resp) != 0) {
		fprintf(stderr, "replica: create_topic forward recv failed\n");
		close(fd);
		return;
	}
	if (resp.status_code != 0)
		fprintf(stderr,
				"replica: create_topic follower status=%d\n",
				resp.status_code);
	else if (broker->log_topic_actions != 0)
		fprintf(stderr,
				"[broker] replica_ack create_topic topic=%.*s partitions=%u\n",
				(int)src->header.topic_length,
				(const char *)src->topic,
				(unsigned)src->header.create_partition_count);
	network_response_deinit(&resp);
	close(fd);
}

static int handle_cmd_replicate(struct broker *broker, struct message *message)
{
	struct topic *target_topic = NULL;
	struct record record;

	if (!broker || !message)
		return -1;
	if (message->header.value_length == 0 || !message->value)
		return -1;

	if (find_topic_from_message(broker, message, &target_topic) != 0)
		return -1;

	memset(&record, 0, sizeof(record));
	if (record_init(&record,
					message->header.timestamp,
					message->header.value_length,
					message->value) != 0)
		return -1;

	record.header.offset = message->header.consume_offset;

	if (topic_replicate_write(
			target_topic, message->header.partition_index, &record) != 0) {
		record_destroy(&record);
		return -1;
	}

	record_destroy(&record);
	return 0;
}

static int serialize_record_payload(const struct record *record,
									uint8_t **payload_out,
									uint32_t *payload_length_out)
{
	if (!record || !payload_out || !payload_length_out)
		return -1;

	uint32_t serialized_record_length =
		(uint32_t)(sizeof(struct record_header) + record->header.value_length);
	uint8_t *serialized_record = malloc(serialized_record_length);
	if (!serialized_record)
		return -1;

	if (record_serialize(record, serialized_record, serialized_record_length) !=
		(ssize_t)serialized_record_length) {
		free(serialized_record);
		return -1;
	}

	*payload_out = serialized_record;
	*payload_length_out = serialized_record_length;
	return 0;
}

static int broker_create_directory_path(const char *path)
{
	char *mutable_path;
	size_t index;

	if (!path || path[0] == '\0')
		return -1;
	mutable_path = strdup(path);
	if (!mutable_path)
		return -1;
	for (index = strlen(mutable_path);
		 index > 1 && mutable_path[index - 1] == '/';
		 index--)
		mutable_path[index - 1] = '\0';

	for (index = 1; mutable_path[index] != '\0'; index++) {
		if (mutable_path[index] != '/')
			continue;
		if (mutable_path[index - 1] == '/')
			continue;
		mutable_path[index] = '\0';
		if (mkdir(mutable_path, 0700) != 0 && errno != EEXIST) {
			free(mutable_path);
			return -1;
		}
		mutable_path[index] = '/';
	}
	if (mkdir(mutable_path, 0700) != 0 && errno != EEXIST) {
		free(mutable_path);
		return -1;
	}
	free(mutable_path);
	return 0;
}

int broker_init(struct broker *broker, const struct broker_config *config)
{
	const char *data_dir;

	if (!broker || !config)
		return -1;
	data_dir = config->data_dir;
	if (!data_dir || data_dir[0] == '\0')
		return -1;
	if (config->max_request_frame_bytes < BROKER_CONFIG_MIN_FRAME_BYTES ||
		config->max_request_frame_bytes > BROKER_CONFIG_MAX_FRAME_BYTES)
		return -1;

	memset(broker, 0, sizeof(*broker));
	broker_connection_table_init(broker);
	broker->server_fd = -1;

	broker->data_dir = strdup(data_dir);
	if (!broker->data_dir)
		return -1;
	broker->port = config->listen_port;
	broker->max_request_frame_size_bytes = config->max_request_frame_bytes;
	broker->log_topic_actions = config->log_topic_actions ? 1 : 0;
	broker->log_consume_miss_actions = config->log_consume_miss_actions ? 1 : 0;
	broker->log_client_io = config->log_client_io ? 1 : 0;

	global_stop_flag = &broker->stop_requested;
	if (signal(SIGINT, handle_sigint) == SIG_ERR)
		return -1;

	if (broker_create_directory_path(data_dir) != 0)
		return -1;

	broker->server_fd = network_listen(broker->port);
	if (broker->server_fd == -1)
		return -1;
	if (network_set_nonblocking(broker->server_fd) != 0)
		return -1;

	broker->replica_host = NULL;
	broker->replica_port = 0;
	if (config->replica_host != NULL && config->replica_host[0] != '\0') {
		broker->replica_host = strdup(config->replica_host);
		if (broker->replica_host == NULL)
			return -1;
		broker->replica_port =
			config->replica_port != 0 ? config->replica_port : broker->port;
	}

	return 0;
}

void broker_destroy(struct broker *broker)
{
	if (!broker)
		return;

	if (broker->server_fd >= 0) {
		close(broker->server_fd);
		broker->server_fd = -1;
	}
	for (size_t connection_index = 0; connection_index < BROKER_MAX_CONNECTIONS;
		 connection_index++) {
		broker_connection_reset_state(&broker->connections[connection_index]);
	}

	for (uint32_t topic_index = 0; topic_index < broker->topic_count;
		 topic_index++) {
		if (broker->topics[topic_index]) {
			topic_destroy(broker->topics[topic_index]);
			free(broker->topics[topic_index]);
			broker->topics[topic_index] = NULL;
		}
	}

	broker->topic_count = 0;
	free(broker->data_dir);
	broker->data_dir = NULL;
	free(broker->replica_host);
	broker->replica_host = NULL;
	broker->stop_requested = -1;
}

static int handle_cmd_produce(struct broker *broker,
							  struct message *message,
							  uint32_t *partition_out,
							  uint64_t *offset_out)
{
	if (!broker || !message)
		return -1;
	if (message->header.value_length == 0 || !message->value)
		return -1;

	struct topic *target_topic = NULL;
	if (find_topic_from_message(broker, message, &target_topic) != 0)
		return -1;

	struct record record;
	memset(&record, 0, sizeof(record));

	if (record_init(&record,
					message->header.timestamp,
					message->header.value_length,
					message->value) != 0) {
		return -1;
	}

	uint32_t assigned_partition = 0;
	int topic_write_result = topic_write(target_topic,
										 message->key,
										 message->header.key_length,
										 &record,
										 &assigned_partition);

	if (topic_write_result == 0) {
		if (partition_out)
			*partition_out = assigned_partition;
		if (offset_out)
			*offset_out = record.header.offset;
		char details[128];
		snprintf(details,
				 sizeof(details),
				 "partition=%u offset=%" PRIu64 " bytes=%u",
				 assigned_partition,
				 record.header.offset,
				 message->header.value_length);
		broker_log_topic_action(broker, "produce", message, details);
		if (broker->replica_host != NULL)
			broker_forward_replicate_produce(
				broker, target_topic, assigned_partition, &record, message);
	}

	record_destroy(&record);
	return topic_write_result;
}

static int handle_cmd_consume(struct broker *broker,
							  struct message *message,
							  uint8_t **response_payload_out,
							  uint32_t *response_payload_length_out)
{
	if (!broker || !message || !response_payload_out ||
		!response_payload_length_out)
		return -1;
	*response_payload_out = NULL;
	*response_payload_length_out = 0;

	struct topic *target_topic = NULL;
	if (find_topic_from_message(broker, message, &target_topic) != 0)
		return -1;

	struct record record;
	memset(&record, 0, sizeof(record));

	if (topic_read(target_topic,
				   &record,
				   message->header.partition_index,
				   message->header.consume_offset) != 0) {
		if (broker->log_consume_miss_actions != 0) {
			char details[128];
			snprintf(details,
					 sizeof(details),
					 "partition=%u offset=%" PRIu64 " status=miss",
					 message->header.partition_index,
					 message->header.consume_offset);
			broker_log_topic_action(broker, "consume", message, details);
		}
		return -1;
	}
	{
		char details[160];
		snprintf(details,
				 sizeof(details),
				 "partition=%u offset=%" PRIu64 " status=hit bytes=%u",
				 message->header.partition_index,
				 message->header.consume_offset,
				 record.header.value_length);
		broker_log_topic_action(broker, "consume", message, details);
	}

	if (serialize_record_payload(
			&record, response_payload_out, response_payload_length_out) != 0) {
		record_destroy(&record);
		return -1;
	}

	record_destroy(&record);
	return 0;
}

static int handle_cmd_create_topic(struct broker *broker,
								   struct message *message)
{
	if (!broker || !message)
		return -1;

	if (message->header.create_partition_count < 1)
		return -1;

	char *topic_name = NULL;
	if (copy_topic_name_from_message(message, &topic_name) != 0)
		return -1;

	if (broker_create_topic(
			broker, topic_name, message->header.create_partition_count) != 0) {
		free(topic_name);
		return -1;
	}
	{
		char details[64];
		snprintf(details,
				 sizeof(details),
				 "partitions=%u",
				 message->header.create_partition_count);
		broker_log_topic_action(broker, "create_topic", message, details);
	}

	free(topic_name);
	broker_forward_replicate_create_topic(broker, message);
	return 0;
}

static int broker_build_response_for_message(struct broker *broker,
											 struct message *message,
											 uint8_t **response_buffer_out,
											 size_t *response_buffer_length_out)
{
	uint8_t *response_payload = NULL;
	uint32_t response_payload_length = 0;
	int response_status_code = 1;

	if (!message)
		return -1;
	if (!response_buffer_out || !response_buffer_length_out)
		return -1;
	*response_buffer_out = NULL;
	*response_buffer_length_out = 0;

	switch (message->header.msg_type) {
	case CMD_PRODUCE: {
		uint32_t produced_partition = 0;
		uint64_t produced_offset = 0;

		if (handle_cmd_produce(
				broker, message, &produced_partition, &produced_offset) == 0) {
			response_status_code = 0;
			response_payload_length = 12;
			response_payload = malloc(response_payload_length);
			if (!response_payload)
				return -1;
			uint8_t *payload_writer = response_payload;

			pack_u32(&payload_writer, produced_partition);
			pack_u64(&payload_writer, produced_offset);
		}
		break;
	}

	case CMD_CONSUME:
		if (handle_cmd_consume(
				broker, message, &response_payload, &response_payload_length) !=
			0)
			break;
		response_status_code = 0;
		break;

	case CMD_CREATE_TOPIC:
		if (handle_cmd_create_topic(broker, message) == 0)
			response_status_code = 0;
		break;

	case CMD_REPLICATE:
		if (handle_cmd_replicate(broker, message) == 0)
			response_status_code = 0;
		break;

	default:
		break;
	}

	if (network_build_response_buffer(response_status_code,
									  response_payload,
									  response_payload_length,
									  response_buffer_out,
									  response_buffer_length_out) != 0) {
		free(response_payload);
		return -1;
	}
	free(response_payload);
	return 0;
}

static short broker_connection_requested_poll_events(
	const struct broker_connection *connection)
{
	short requested_events = 0;

	if (!connection || !connection->is_in_use)
		return 0;
	if (connection->phase == BROKER_CONNECTION_PHASE_READ_HEADER ||
		connection->phase == BROKER_CONNECTION_PHASE_READ_BODY) {
		requested_events |= POLLIN;
	}
	if (connection->phase == BROKER_CONNECTION_PHASE_WRITE_RESPONSE)
		requested_events |= POLLOUT;
	return requested_events;
}

static nfds_t broker_build_poll_descriptors(
	struct broker *broker,
	struct pollfd poll_fds[1 + BROKER_MAX_CONNECTIONS],
	struct broker_connection *poll_connections[BROKER_MAX_CONNECTIONS])
{
	nfds_t poll_count = 1;

	memset(poll_fds, 0, sizeof(struct pollfd) * (1 + BROKER_MAX_CONNECTIONS));
	memset(poll_connections,
		   0,
		   sizeof(struct broker_connection *) * BROKER_MAX_CONNECTIONS);

	poll_fds[0].fd = broker->server_fd;
	poll_fds[0].events = POLLIN;

	for (size_t connection_index = 0; connection_index < BROKER_MAX_CONNECTIONS;
		 connection_index++) {
		struct broker_connection *connection =
			&broker->connections[connection_index];
		if (!connection->is_in_use)
			continue;
		poll_fds[poll_count].fd = connection->fd;
		poll_fds[poll_count].events =
			broker_connection_requested_poll_events(connection);
		poll_connections[poll_count - 1] = connection;
		poll_count++;
	}

	return poll_count;
}

static void broker_accept_pending_connections(struct broker *broker)
{
	while (1) {
		int accepted_client_fd = network_accept(broker->server_fd);
		if (accepted_client_fd < 0) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			break;
		}

		struct broker_connection *connection =
			broker_connection_acquire_free_slot(broker);
		if (!connection) {
			close(accepted_client_fd);
			continue;
		}

		connection->fd = accepted_client_fd;
		if (network_set_nonblocking(connection->fd) != 0)
			broker_connection_reset_state(connection);
		else if (broker->log_client_io)
			fprintf(
				stderr, "[broker] client connected fd=%d\n", connection->fd);
	}
}

static int
broker_decode_and_handle_complete_request(struct broker *broker,
										  struct broker_connection *connection)
{
	struct message message;
	uint8_t *response_buffer = NULL;
	size_t response_buffer_length = 0;

	if (network_decode_packet_buffers(connection->incoming_header_buffer,
									  connection->incoming_body_buffer,
									  connection->incoming_body_length,
									  &message) != 0)
		return -1;

	if (broker_build_response_for_message(
			broker, &message, &response_buffer, &response_buffer_length) != 0) {
		message_destroy(&message);
		return -1;
	}

	message_destroy(&message);
	free(connection->incoming_body_buffer);
	connection->incoming_body_buffer = NULL;
	connection->incoming_body_length = 0;
	connection->incoming_body_bytes_received = 0;
	connection->outgoing_response_buffer = response_buffer;
	connection->outgoing_response_length = response_buffer_length;
	connection->outgoing_response_bytes_sent = 0;
	connection->phase = BROKER_CONNECTION_PHASE_WRITE_RESPONSE;
	return 0;
}

static int broker_try_read_request_header(struct broker *broker,
										  struct broker_connection *connection)
{
	if (!broker || !connection ||
		connection->phase != BROKER_CONNECTION_PHASE_READ_HEADER)
		return 0;

	enum network_io_result read_result = network_recv_into_buffer_step(
		connection->fd,
		connection->incoming_header_buffer,
		NETWORK_HEADER_WIRE_SIZE,
		&connection->incoming_header_bytes_received);

	if (read_result == NETWORK_IO_PEER_CLOSED ||
		read_result == NETWORK_IO_ERROR)
		return -1;
	if (read_result != NETWORK_IO_COMPLETE)
		return 0;

	if (broker_parse_expected_body_length(broker,
										  connection->incoming_header_buffer,
										  &connection->incoming_body_length) !=
		0) {
		return -1;
	}

	connection->phase = BROKER_CONNECTION_PHASE_READ_BODY;
	if (connection->incoming_body_length == 0)
		return 1;

	connection->incoming_body_buffer = malloc(connection->incoming_body_length);
	if (!connection->incoming_body_buffer)
		return -1;
	return 0;
}

static int broker_try_read_request_body(struct broker *broker,
										struct broker_connection *connection)
{
	if (!broker || !connection ||
		connection->phase != BROKER_CONNECTION_PHASE_READ_BODY) {
		return 0;
	}

	enum network_io_result read_body_result = NETWORK_IO_COMPLETE;
	if (connection->incoming_body_length > 0) {
		read_body_result = network_recv_into_buffer_step(
			connection->fd,
			connection->incoming_body_buffer,
			connection->incoming_body_length,
			&connection->incoming_body_bytes_received);
	}

	if (read_body_result == NETWORK_IO_PEER_CLOSED ||
		read_body_result == NETWORK_IO_ERROR) {
		return -1;
	}
	if (read_body_result != NETWORK_IO_COMPLETE)
		return 0;

	return broker_decode_and_handle_complete_request(broker, connection);
}

static int broker_try_flush_response(struct broker_connection *connection)
{
	if (!connection ||
		connection->phase != BROKER_CONNECTION_PHASE_WRITE_RESPONSE) {
		return 0;
	}

	enum network_io_result write_result = network_send_from_buffer_step(
		connection->fd,
		connection->outgoing_response_buffer,
		connection->outgoing_response_length,
		&connection->outgoing_response_bytes_sent);

	if (write_result == NETWORK_IO_PEER_CLOSED ||
		write_result == NETWORK_IO_ERROR)
		return -1;
	if (write_result == NETWORK_IO_COMPLETE)
		broker_connection_prepare_for_next_request(connection);
	return 0;
}

static void broker_process_connection_events(
	struct broker *broker, struct broker_connection *connection, short revents)
{
	if (!broker || !connection || !connection->is_in_use)
		return;
	if ((revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
		if (broker->log_client_io)
			fprintf(
				stderr, "[broker] client disconnected fd=%d\n", connection->fd);
		broker_connection_reset_state(connection);
		return;
	}

	if ((revents & POLLIN) != 0) {
		int header_read_step_result =
			broker_try_read_request_header(broker, connection);
		if (header_read_step_result < 0) {
			if (broker->log_client_io)
				fprintf(stderr,
						"[broker] client read error fd=%d\n",
						connection->fd);
			broker_connection_reset_state(connection);
			return;
		}

		int body_read_step_result =
			broker_try_read_request_body(broker, connection);
		if (body_read_step_result < 0) {
			if (broker->log_client_io)
				fprintf(stderr,
						"[broker] client request error fd=%d\n",
						connection->fd);
			broker_connection_reset_state(connection);
			return;
		}
	}

	if ((revents & POLLOUT) != 0) {
		if (broker_try_flush_response(connection) != 0) {
			if (broker->log_client_io)
				fprintf(stderr,
						"[broker] client write error fd=%d\n",
						connection->fd);
			broker_connection_reset_state(connection);
		}
	}
}

int broker_run(struct broker *broker)
{
	struct pollfd poll_fds[1 + BROKER_MAX_CONNECTIONS];
	struct broker_connection *poll_connections[BROKER_MAX_CONNECTIONS];

	if (!broker)
		return -1;

	while (!broker->stop_requested) {
		nfds_t poll_count =
			broker_build_poll_descriptors(broker, poll_fds, poll_connections);

		int poll_result = poll(poll_fds, poll_count, 250);
		if (poll_result < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (poll_result == 0)
			continue;

		if ((poll_fds[0].revents & POLLIN) != 0)
			broker_accept_pending_connections(broker);

		for (nfds_t poll_index = 1; poll_index < poll_count; poll_index++) {
			struct broker_connection *connection =
				poll_connections[poll_index - 1];
			short revents = poll_fds[poll_index].revents;
			broker_process_connection_events(broker, connection, revents);
		}
	}

	return 0;
}

int broker_create_topic(struct broker *broker,
						const char *name,
						uint32_t partition_count)
{
	struct topic *existing;

	if (!broker || !name)
		return -1;

	existing = broker_find_topic(broker, name);
	if (existing != NULL) {
		if (existing->partition_count == partition_count)
			return 0;
		return -1;
	}

	if (broker->topic_count >= BROKER_MAX_TOPICS)
		return -1;

	struct topic *topic = malloc(sizeof(struct topic));
	if (!topic)
		return -1;

	if (topic_init(topic, name, broker->data_dir, partition_count) != 0) {
		free(topic);
		return -1;
	}
	broker->topics[broker->topic_count] = topic;
	broker->topic_count++;

	return 0;
}

struct topic *broker_find_topic(struct broker *broker, const char *target_name)
{
	if (!broker || !target_name)
		return NULL;

	for (uint32_t topic_index = 0; topic_index < broker->topic_count;
		 topic_index++) {
		struct topic *candidate = broker->topics[topic_index];
		if (!candidate || !candidate->name)
			continue;

		if (strcmp(candidate->name, target_name) == 0) {
			return candidate;
		}
	}

	return NULL;
}
