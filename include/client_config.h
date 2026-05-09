#ifndef CLIENT_CONFIG_H
#define CLIENT_CONFIG_H

#include <stdint.h>

#include "client.h"

struct broker_client_settings {
	char *broker_host;
	char *broker_hosts;
	uint16_t broker_port;
};

void broker_client_settings_set_defaults(
	struct broker_client_settings *settings);

void broker_client_settings_destroy(struct broker_client_settings *settings);

/*
 * returns 0 on success, -1 on invalid known key syntax, -2 ENOENT,
 * -3 other read errors from fopen/ferror
 */
int broker_client_settings_merge_file(const char *path,
									  struct broker_client_settings *settings);

int broker_client_connect_with_settings(
	struct broker_client *client,
	const struct broker_client_settings *settings);

int broker_client_connect_via_config_file(struct broker_client *client,
										  const char *config_path_option);

#endif
