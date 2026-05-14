#include <errno.h>
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
	for (i = 0; i < BROKER_CLIENT_MAX_HOSTS; i++) {
		client->hosts[i] = NULL;
		client->endpoint_ports[i] = 0;
	}
}

static void broker_client_apply_stream_timeouts_for_failover(int socket_fd)
{
	const unsigned int idle_peer_io_deadline_seconds = 30U;

	if (socket_fd < 0)
		return;
	(void)network_set_stream_socket_io_timeouts(socket_fd,
											 idle_peer_io_deadline_seconds);
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
		client->endpoint_ports[i] = 0;
	}
	client->host_count = 0;
	client->host_index = 0;
}

static int broker_client_parse_decimal_port(const char *port_text,
											uint16_t *port_out)
{
	char *end_pointer = NULL;
	unsigned long parsed;

	if (!port_text || !port_out || port_text[0] == '\0')
		return -1;
	errno = 0;
	parsed = strtoul(port_text, &end_pointer, 10);
	if (errno != 0 || end_pointer == port_text || *end_pointer != '\0')
		return -1;
	if (parsed == 0UL || parsed > 65535UL)
		return -1;
	*port_out = (uint16_t)parsed;
	return 0;
}

static int broker_client_split_host_port_spec(const char *spec,
											  char **hostname_out,
											  uint16_t *port_out)
{
	const char *colon;
	size_t host_byte_count;
	char *host_heap;

	if (!spec || !hostname_out || !port_out || spec[0] == '\0')
		return -1;
	*hostname_out = NULL;

	if (spec[0] == '[') {
		const char *close_bracket = strchr(spec + 1, ']');

		if (!close_bracket || close_bracket == spec + 1)
			return -1;
		host_byte_count = (size_t)(close_bracket - (spec + 1));
		if (close_bracket[1] == '\0')
			return -1;
		if (close_bracket[1] != ':' || close_bracket[2] == '\0')
			return -1;
		if (broker_client_parse_decimal_port(close_bracket + 2, port_out) != 0)
			return -1;
		host_heap = malloc(host_byte_count + 1);
		if (!host_heap)
			return -1;
		memcpy(host_heap, spec + 1, host_byte_count);
		host_heap[host_byte_count] = '\0';
		*hostname_out = host_heap;
		return 0;
	}

	colon = strchr(spec, ':');
	if (!colon)
		return -1;
	if (strchr(colon + 1, ':') != NULL)
		return -1;
	if (colon == spec)
		return -1;
	if (colon[1] == '\0')
		return -1;
	if (broker_client_parse_decimal_port(colon + 1, port_out) != 0)
		return -1;
	host_byte_count = (size_t)(colon - spec);
	host_heap = malloc(host_byte_count + 1);
	if (!host_heap)
		return -1;
	memcpy(host_heap, spec, host_byte_count);
	host_heap[host_byte_count] = '\0';
	*hostname_out = host_heap;
	return 0;
}

static void broker_client_trim_token_inplace(char *token)
{
	char *end;

	while (*token == ' ' || *token == '\t')
		token++;
	end = token + strlen(token);
	while (end > token && (end[-1] == ' ' || end[-1] == '\t'))
		*--end = '\0';
}

static int broker_client_push_parsed_endpoint(struct broker_client *client,
											  const char *token)
{
	char *hostname = NULL;
	uint16_t port_value;

	if (!client || !token || token[0] == '\0')
		return -1;
	if (client->host_count >= BROKER_CLIENT_MAX_HOSTS)
		return -1;
	if (broker_client_split_host_port_spec(token, &hostname, &port_value) !=
		0) {
		free(hostname);
		return -1;
	}
	client->hosts[client->host_count] = hostname;
	client->endpoint_ports[client->host_count] = port_value;
	client->host_count++;
	return 0;
}

int broker_client_connect(struct broker_client *client,
						  const char *host_port_spec)
{
	int sockfd;

	if (!client || !host_port_spec || host_port_spec[0] == '\0')
		return -1;

	broker_client_close(client);

	if (broker_client_push_parsed_endpoint(client, host_port_spec) != 0) {
		broker_client_close(client);
		return -1;
	}

	sockfd = network_connect(client->hosts[0], client->endpoint_ports[0]);
	if (sockfd < 0) {
		broker_client_close(client);
		return -1;
	}

	client->fd = sockfd;
	broker_client_apply_stream_timeouts_for_failover(sockfd);
	client->host_index = 0;
	return 0;
}

int broker_client_connect_hosts(struct broker_client *client,
								const char *comma_separated_host_port_specs)
{
	char *buffer = NULL;
	char *cursor = NULL;
	char *saveptr = NULL;

	if (!client || !comma_separated_host_port_specs ||
		comma_separated_host_port_specs[0] == '\0')
		return -1;

	broker_client_close(client);

	buffer = strdup(comma_separated_host_port_specs);
	if (!buffer)
		return -1;

	for (cursor = strtok_r(buffer, ",", &saveptr);
		 cursor != NULL && client->host_count < BROKER_CLIENT_MAX_HOSTS;
		 cursor = strtok_r(NULL, ",", &saveptr)) {
		broker_client_trim_token_inplace(cursor);
		if (cursor[0] == '\0')
			continue;
		if (broker_client_push_parsed_endpoint(client, cursor) != 0) {
			free(buffer);
			broker_client_close(client);
			return -1;
		}
	}
	free(buffer);

	if (client->host_count == 0) {
		broker_client_close(client);
		return -1;
	}

	for (size_t i = 0; i < client->host_count; i++) {
		int sockfd =
			network_connect(client->hosts[i], client->endpoint_ports[i]);

		if (sockfd >= 0) {
			client->fd = sockfd;
			broker_client_apply_stream_timeouts_for_failover(sockfd);
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
		int sockfd =
			network_connect(client->hosts[idx], client->endpoint_ports[idx]);

		if (sockfd >= 0) {
			client->fd = sockfd;
			broker_client_apply_stream_timeouts_for_failover(sockfd);
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

static size_t
broker_client_failover_attempt_limit(const struct broker_client *client)
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
