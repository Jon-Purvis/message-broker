#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "../include/broker.h"

int main(void)
{
	struct broker broker;
	const char *data_dir = getenv("BROKER_DATA_DIR");
	const char *port_env = getenv("BROKER_PORT");
	uint16_t port = 3490;

	if (data_dir == NULL || data_dir[0] == '\0')
		data_dir = "./data";

	if (port_env != NULL && port_env[0] != '\0') {
		unsigned long parsed = strtoul(port_env, NULL, 10);

		if (parsed == 0UL || parsed > 65535UL) {
			fprintf(stderr, "BROKER_PORT must be 1-65535\n");
			return 1;
		}
		port = (uint16_t)parsed;
	}

	if (broker_init(&broker, data_dir, port) != 0) {
		fprintf(stderr, "failed to initialize broker\n");
		return 1;
	}

	int result = broker_run(&broker);
	broker_destroy(&broker);
	if (result == 0)
		return 0;
	return -1;
}
