#ifndef BROKER_H
#define BROKER_H

#include <signal.h>
#include <stdint.h>
#include "topic.h"

#define BROKER_MAX_TOPICS 64

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

int broker_create_topic(struct broker *broker,
						const char *name,
						uint32_t partition_count);

struct topic *broker_find_topic(struct broker *broker, const char *target_name);

#endif
