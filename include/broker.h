#ifndef BROKER_H
#define BROKER_H

#include <signal.h>
#include <stdint.h>
#include "topic.h"

#define BROKER_MAX_TOPICS 64

/*
 * Concurrent clients: broker_run uses poll(2) on the listen socket plus every
 * accepted TCP connection (see MAX_CLIENTS in broker.c). Many machines can
 * stay connected; each request is handled one-at-a-time on the broker thread
 * (no worker pool), so heavy produce/consume work delays other clients until
 * that handler returns.
 */

struct broker {
	struct topic *topics[BROKER_MAX_TOPICS];
	uint32_t topic_count;
	char *data_dir;
	int server_fd;
	uint16_t port;
	volatile sig_atomic_t stop_requested;
};

int broker_init(struct broker *broker, const char *data_dir, uint16_t port);

void broker_destroy(struct broker *broker);

int broker_run(struct broker *broker);

int broker_create_topic(struct broker *broker, const char *name,
		uint32_t partition_count);

struct topic *broker_find_topic(struct broker *broker, const char *name);

#endif

