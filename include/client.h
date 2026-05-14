#ifndef CLIENT_H
#define CLIENT_H

#include <stddef.h>
#include <stdint.h>

#include "network.h"

#define BROKER_CLIENT_MAX_HOSTS 8

struct broker_client {
	int fd;
	char *hosts[BROKER_CLIENT_MAX_HOSTS];
	uint16_t endpoint_ports[BROKER_CLIENT_MAX_HOSTS];
	size_t host_count;
	size_t host_index;
};

void broker_client_init(struct broker_client *client);

int broker_client_connect(struct broker_client *client,
						  const char *host_port_spec);

int broker_client_connect_hosts(struct broker_client *client,
								const char *comma_separated_host_port_specs);

void broker_client_close(struct broker_client *client);

/*
 * returns 0 when the request was sent and a response frame was read.
 * status_out is the broker status (0 success).
 */
int broker_client_create_topic(struct broker_client *client,
							   const char *topic,
							   uint32_t partitions,
							   int *status_out);

/*
 * on I/O success with *status_out==0, parses the 12-byte produce ack:
 * partition (BE u32), offset (BE u64).
 */
int broker_client_produce(struct broker_client *client,
						  const char *topic,
						  const void *key,
						  size_t key_len,
						  const void *value,
						  size_t value_len,
						  int *status_out,
						  uint32_t *partition_out,
						  uint64_t *offset_out);

/*
 * caller must network_response_deinit(resp_out) after use.
 */
int broker_client_consume(struct broker_client *client,
						  const char *topic,
						  uint32_t partition,
						  uint64_t offset,
						  int *status_out,
						  struct network_response *resp_out);

#endif
