#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../include/client.h"
#include "../include/util.h"

void broker_client_init(struct broker_client *client)
{
	size_t i;

	if (!client)
		return;
	client->fd = -1;
	client->host_count = 0;
	client->host_index = 0;
	client->port = 0;
	for (i = 0; i < BROKER_CLIENT_MAX_HOSTS; i++)
		client->hosts[i] = NULL;
}

void broker_client_close(struct broker_client *client)
{
	size_t i;

	if (!client)
		return;
	if (client->fd >= 0) {
		close(client->fd);
		client->fd = -1;
	}
	for (i = 0; i < BROKER_CLIENT_MAX_HOSTS; i++) {
		free(client->hosts[i]);
		client->hosts[i] = NULL;
	}
	client->host_count = 0;
	client->host_index = 0;
}

int broker_client_connect(struct broker_client *client,
						  const char *host,
						  uint16_t port)
{
	int sockfd;

	if (!client || !host)
		return -1;

	broker_client_close(client);

	client->hosts[0] = strdup(host);
	if (!client->hosts[0])
		return -1;
	client->host_count = 1;
	client->host_index = 0;
	client->port = port;

	sockfd = network_connect(host, port);
	if (sockfd < 0) {
		broker_client_close(client);
		return -1;
	}

	client->fd = sockfd;
	return 0;
}

static void broker_client_trim_host_token(char *token)
{
	char *end;

	while (*token == ' ' || *token == '\t')
		token++;
	end = token + strlen(token);
	while (end > token && (end[-1] == ' ' || end[-1] == '\t'))
		*--end = '\0';
}

int broker_client_connect_hosts(struct broker_client *client,
								const char *comma_separated_hosts,
								uint16_t port)
{
	char *buffer = NULL;
	char *cursor = NULL;
	char *saveptr = NULL;

	if (!client || !comma_separated_hosts || comma_separated_hosts[0] == '\0')
		return -1;

	broker_client_close(client);
	client->port = port;

	buffer = strdup(comma_separated_hosts);
	if (!buffer)
		return -1;

	for (cursor = strtok_r(buffer, ",", &saveptr);
		 cursor != NULL && client->host_count < BROKER_CLIENT_MAX_HOSTS;
		 cursor = strtok_r(NULL, ",", &saveptr)) {
		char *host_copy;

		broker_client_trim_host_token(cursor);
		if (cursor[0] == '\0')
			continue;

		host_copy = strdup(cursor);
		if (!host_copy) {
			free(buffer);
			broker_client_close(client);
			return -1;
		}
		client->hosts[client->host_count++] = host_copy;
	}
	free(buffer);

	if (client->host_count == 0)
		return -1;

	for (size_t i = 0; i < client->host_count; i++) {
		int sockfd = network_connect(client->hosts[i], client->port);

		if (sockfd >= 0) {
			client->fd = sockfd;
			client->host_index = i;
			return 0;
		}
	}

	broker_client_close(client);
	return -1;
}

static int broker_client_failover_to_next_host(struct broker_client *client)
{
	if (!client || client->host_count <= 1)
		return -1;

	if (client->fd >= 0) {
		close(client->fd);
		client->fd = -1;
	}

	for (size_t step = 1; step < client->host_count; step++) {
		size_t idx = (client->host_index + step) % client->host_count;
		int sockfd = network_connect(client->hosts[idx], client->port);

		if (sockfd >= 0) {
			client->fd = sockfd;
			client->host_index = idx;
			return 0;
		}
	}

	return -1;
}

static int broker_client_do_request(struct broker_client *client,
									struct message *msg,
									struct network_response *resp_out)
{
	if (!client || !msg || !resp_out)
		return -1;

	memset(resp_out, 0, sizeof(*resp_out));

	if (client->fd < 0)
		return -1;

	if (network_send_packet(client->fd, msg) != 0) {
		message_destroy(msg);
		return -1;
	}
	message_destroy(msg);

	if (network_recv_response(client->fd, resp_out) != 0)
		return -1;

	return 0;
}

static size_t broker_client_failover_attempt_limit(const struct broker_client *client)
{
	if (!client || client->host_count == 0)
		return 1;
	return client->host_count;
}

int broker_client_create_topic(struct broker_client *client,
							   const char *topic,
							   uint32_t partitions,
							   int *status_out)
{
	size_t topic_len;

	if (!client || !topic || !status_out)
		return -1;

	topic_len = strlen(topic);
	if (topic_len == 0 || topic_len > UINT32_MAX)
		return -1;

	for (size_t attempt = 0;
		 attempt < broker_client_failover_attempt_limit(client);
		 attempt++) {
		struct message msg;

		if (message_init(&msg,
						 CMD_CREATE_TOPIC,
						 0,
						 topic,
						 (uint32_t)topic_len,
						 NULL,
						 0,
						 NULL,
						 0,
						 0,
						 0,
						 partitions) != 0)
			return -1;

		struct network_response resp;
		int req_result = broker_client_do_request(client, &msg, &resp);

		if (req_result == 0) {
			*status_out = resp.status_code;
			network_response_deinit(&resp);
			return 0;
		}

		network_response_deinit(&resp);
		if (broker_client_failover_to_next_host(client) != 0)
			return -1;
	}

	return -1;
}

int broker_client_produce(struct broker_client *client,
						  const char *topic,
						  const void *key,
						  size_t key_len,
						  const void *value,
						  size_t value_len,
						  int *status_out,
						  uint32_t *partition_out,
						  uint64_t *offset_out)
{
	size_t topic_len;

	if (!client || !topic || !value || !status_out || !partition_out ||
		!offset_out)
		return -1;
	if (value_len == 0 || value_len > UINT32_MAX)
		return -1;
	if (key_len > UINT32_MAX)
		return -1;

	topic_len = strlen(topic);
	if (topic_len == 0 || topic_len > UINT32_MAX)
		return -1;

	for (size_t attempt = 0;
		 attempt < broker_client_failover_attempt_limit(client);
		 attempt++) {
		struct message msg;

		if (message_init(&msg,
						 CMD_PRODUCE,
						 0,
						 topic,
						 (uint32_t)topic_len,
						 key,
						 (uint32_t)key_len,
						 value,
						 (uint32_t)value_len,
						 0,
						 0,
						 0) != 0)
			return -1;

		struct network_response resp;
		int req_result = broker_client_do_request(client, &msg, &resp);

		if (req_result == 0) {
			*status_out = resp.status_code;
			if (*status_out == 0) {
				if (!resp.body || resp.body_length != 12) {
					network_response_deinit(&resp);
					return -1;
				}
				const uint8_t *reader = resp.body;

				*partition_out = unpack_u32(&reader);
				*offset_out = unpack_u64(&reader);
			} else {
				*partition_out = 0;
				*offset_out = 0;
			}

			network_response_deinit(&resp);
			return 0;
		}

		network_response_deinit(&resp);
		if (broker_client_failover_to_next_host(client) != 0)
			return -1;
	}

	return -1;
}

int broker_client_consume(struct broker_client *client,
						  const char *topic,
						  uint32_t partition,
						  uint64_t offset,
						  int *status_out,
						  struct network_response *resp_out)
{
	size_t topic_len;

	if (!client || !topic || !status_out || !resp_out)
		return -1;

	topic_len = strlen(topic);
	if (topic_len == 0 || topic_len > UINT32_MAX)
		return -1;

	for (size_t attempt = 0;
		 attempt < broker_client_failover_attempt_limit(client);
		 attempt++) {
		struct message msg;

		if (message_init(&msg,
						 CMD_CONSUME,
						 0,
						 topic,
						 (uint32_t)topic_len,
						 NULL,
						 0,
						 NULL,
						 0,
						 partition,
						 offset,
						 0) != 0)
			return -1;

		int req_result = broker_client_do_request(client, &msg, resp_out);

		if (req_result == 0) {
			*status_out = resp_out->status_code;
			return 0;
		}

		network_response_deinit(resp_out);
		if (broker_client_failover_to_next_host(client) != 0)
			return -1;
	}

	return -1;
}
