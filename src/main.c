#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/broker.h"
#include "../include/broker_config.h"

static void print_usage(const char *argv0)
{
	fprintf(stderr,
			"Usage: %s [broker.conf]\n"
			"  With no arguments, reads ./broker.conf when present; otherwise "
			"defaults apply.\n",
			argv0);
}

int main(int argc, char **argv)
{
	struct broker broker_instance;
	struct broker_config config;
	const char *config_path;
	int merge_status;

	if (argc > 2) {
		fprintf(stderr, "%s: too many arguments\n", argv[0]);
		print_usage(argv[0]);
		return 1;
	}
	if (argc == 2 && (strcmp(argv[1], "-h") == 0 ||
					  strcmp(argv[1], "--help") == 0)) {
		print_usage(argv[0]);
		return 0;
	}

	broker_config_set_defaults(&config);
	config_path = (argc == 2) ? argv[1] : "broker.conf";

	merge_status = broker_config_merge_file(config_path, &config);
	if (merge_status == -1) {
		fprintf(stderr,
				"%s: invalid configuration in %s\n",
				argv[0],
				config_path);
		broker_config_destroy(&config);
		return 1;
	}
	if (merge_status == -3) {
		fprintf(stderr, "%s: cannot read %s\n", argv[0], config_path);
		broker_config_destroy(&config);
		return 1;
	}
	if (merge_status == -2 && argc == 2) {
		fprintf(stderr,
				"%s: configuration file not found: %s\n",
				argv[0],
				config_path);
		broker_config_destroy(&config);
		return 1;
	}

	if (!config.data_dir || config.data_dir[0] == '\0') {
		fprintf(stderr, "%s: data_dir must be non-empty\n", argv[0]);
		broker_config_destroy(&config);
		return 1;
	}

	if (broker_init(&broker_instance, &config) != 0) {
		fprintf(stderr, "failed to initialize broker\n");
		broker_config_destroy(&config);
		return 1;
	}
	broker_config_destroy(&config);

	{
		int result = broker_run(&broker_instance);

		broker_destroy(&broker_instance);
		if (result == 0)
			return 0;
		return -1;
	}
}
