#include <stdio.h>
#include "../include/broker.h"

int main(void)
{
	struct broker broker;

	if (broker_init(&broker, "./data", 3490) != 0) {
		fprintf(stderr, "failed to initialize broker\n");
		return 1;
	}

	int result = broker_run(&broker);
	broker_destroy(&broker);
	if (result == 0)
		return 0;
	return -1;
}
