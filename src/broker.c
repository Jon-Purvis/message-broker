#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/broker.h"
#include "../include/network.h"

static volatile sig_atomic_t *global_stop_flag = NULL;

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

int broker_init(struct broker *broker, const char *data_dir, uint16_t port)
{
	if (!broker || !data_dir)
		return -1;

	memset(broker, 0, sizeof(*broker));

	broker->data_dir = strdup(data_dir);
	if (!broker->data_dir)
		return -1;
	broker->port = port;

	global_stop_flag = &broker->stop_requested;
	if (signal(SIGINT, handle_sigint) == SIG_ERR)
		return -1;

	int result = mkdir(data_dir, 0700);
	if (result == -1)
		if (errno != EEXIST)
			return -1;

	broker->server_fd = network_listen(broker->port);
	if (broker->server_fd == -1)
		return -1;

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
	broker->stop_requested = -1;
}

int handle_cmd_produce(struct broker *broker, struct message *message)
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

	int topic_write_result = topic_write(
		target_topic, message->key, message->header.key_length, &record, NULL);

	record_destroy(&record);
	return topic_write_result;
}

int handle_cmd_consume(struct broker *broker,
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
		return -1;
	}

	if (serialize_record_payload(
			&record, response_payload_out, response_payload_length_out) != 0) {
		record_destroy(&record);
		return -1;
	}

	record_destroy(&record);
	return 0;
}

int handle_cmd_create_topic(struct broker *broker, struct message *message)
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

	free(topic_name);
	return 0;
}

int handle_message(struct broker *broker,
				   int client_fd,
				   struct message *message)
{
	if (!message)
		return -1;

	switch (message->header.msg_type) {
	case CMD_PRODUCE:
		if (handle_cmd_produce(broker, message) != 0) {
			network_send_response(client_fd, 1, NULL, 0);
			return -1;
		}
		network_send_response(client_fd, 0, NULL, 0);
		break;

	case CMD_CONSUME: {
		uint8_t *response_payload = NULL;
		uint32_t response_payload_length = 0;
		if (handle_cmd_consume(
				broker, message, &response_payload, &response_payload_length) !=
			0) {
			network_send_response(client_fd, 1, NULL, 0);
			return -1;
		}
		network_send_response(
			client_fd, 0, response_payload, response_payload_length);
		free(response_payload);
		break;
	}

	case CMD_CREATE_TOPIC:
		if (handle_cmd_create_topic(broker, message) != 0) {
			network_send_response(client_fd, 1, NULL, 0);
			return -1;
		}
		network_send_response(client_fd, 0, NULL, 0);
		break;

	default:
		network_send_response(client_fd, 1, NULL, 0);
		return -1;
	}
	return 0;
}

int broker_run(struct broker *broker)
{
	if (!broker)
		return -1;

	int client_fd = -1;
	while (!broker->stop_requested) {
		client_fd = network_accept(broker->server_fd);
		if (client_fd < 0) {
			continue;
		}

		struct message message;
		if (network_recv_packet(client_fd, &message) != 0) {
			close(client_fd);
			continue;
		}

		int handle_result = handle_message(broker, client_fd, &message);
		message_destroy(&message);
		close(client_fd);
		if (handle_result != 0)
			continue;
	}

	return 0;
}

int broker_create_topic(struct broker *broker,
						const char *name,
						uint32_t partition_count)
{
	if (!broker || !name)
		return -1;
	if (broker->topic_count >= BROKER_MAX_TOPICS)
		return -1;
	if (broker_find_topic(broker, name) != NULL)
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
