#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/client.h"
#include "../include/record.h"

static void trim_newline(char *s)
{
	size_t length;

	if (!s)
		return;
	length = strlen(s);
	while (length > 0 && (s[length - 1] == '\n' || s[length - 1] == '\r')) {
		s[length - 1] = '\0';
		length--;
	}
}

static void read_line(const char *prompt, char *buf, size_t buflen)
{
	int read_cap;

	if (buflen > (size_t)(INT_MAX / 4))
		read_cap = INT_MAX / 4;
	else
		read_cap = (int)buflen;
	if (read_cap < 2)
		read_cap = 2;

	printf("%s", prompt);
	if (!fgets(buf, read_cap, stdin))
		buf[0] = '\0';
	else
		trim_newline(buf);
}

static uint32_t read_u32_default(const char *prompt, uint32_t default_val)
{
	char line[64];

	read_line(prompt, line, sizeof line);
	if (line[0] == '\0')
		return default_val;
	return (uint32_t)strtoul(line, NULL, 10);
}

static uint64_t read_u64_default(const char *prompt, uint64_t default_val)
{
	char line[64];

	read_line(prompt, line, sizeof line);
	if (line[0] == '\0')
		return default_val;
	return (uint64_t)strtoull(line, NULL, 10);
}

static int require_connected(struct broker_client *client)
{
	if (!client || client->fd < 0) {
		printf("Not connected.\n");
		return -1;
	}
	return 0;
}

int main(void)
{
	struct broker_client client;
	char host_saved[256] = "127.0.0.1";
	uint16_t port_saved = 3490;

	broker_client_init(&client);

	for (;;) {
		printf("\n--- Message broker CLI ---\n");
		printf("1) Connect\n");
		printf("2) Create topic\n");
		printf("3) Produce\n");
		printf("4) Consume\n");
		printf("5) Disconnect\n");
		printf("6) Quit\n");
		printf("Choice: ");

		char choice_line[64];
		if (!fgets(choice_line, sizeof choice_line, stdin))
			break;

		const int menu_choice = atoi(choice_line);

		switch (menu_choice) {
		case 1: {
			char host_input[256];
			char port_input[64];

			read_line("Host [127.0.0.1]: ", host_input, sizeof host_input);
			if (host_input[0] != '\0') {
				strncpy(host_saved, host_input, sizeof host_saved - 1);
				host_saved[sizeof host_saved - 1] = '\0';
			}
			read_line("Port [3490]: ", port_input, sizeof port_input);
			if (port_input[0] != '\0')
				port_saved = (uint16_t)strtoul(port_input, NULL, 10);

			if (broker_client_connect(&client, host_saved, port_saved) != 0)
				fprintf(stderr, "connect failed\n");
			else
				printf("Connected to %s:%u\n", host_saved,
					   (unsigned)port_saved);
			break;
		}
		case 2: {
			if (require_connected(&client) != 0)
				break;

			char topic_buf[512];
			read_line("Topic name: ", topic_buf, sizeof topic_buf);
			if (topic_buf[0] == '\0') {
				printf("Topic required.\n");
				break;
			}

			uint32_t partitions =
				read_u32_default("Partition count [1]: ", 1);
			int status_code;

			if (broker_client_create_topic(
					&client, topic_buf, partitions, &status_code) != 0) {
				fprintf(stderr, "transport error\n");
				break;
			}
			printf("status=%d (0 ok)\n", status_code);
			break;
		}
		case 3: {
			if (require_connected(&client) != 0)
				break;

			char topic_buf[512];
			char key_buf[512];
			char value_buf[4096];

			read_line("Topic: ", topic_buf, sizeof topic_buf);
			read_line("Key (empty ok): ", key_buf, sizeof key_buf);
			read_line("Value: ", value_buf, sizeof value_buf);

			if (topic_buf[0] == '\0' || value_buf[0] == '\0') {
				printf("Topic and value are required.\n");
				break;
			}

			const void *key_ptr = NULL;
			size_t key_length = 0;

			if (key_buf[0] != '\0') {
				key_ptr = key_buf;
				key_length = strlen(key_buf);
			}

			int status_code;
			uint32_t partition_index;
			uint64_t record_offset;

			if (broker_client_produce(&client,
									  topic_buf,
									  key_ptr,
									  key_length,
									  value_buf,
									  strlen(value_buf),
									  &status_code,
									  &partition_index,
									  &record_offset) != 0) {
				fprintf(stderr, "transport error\n");
				break;
			}

			if (status_code == 0)
				printf("ok partition=%" PRIu32 " offset=%" PRIu64 "\n",
					   partition_index, record_offset);
			else
				printf("produce failed status=%d\n", status_code);
			break;
		}
		case 4: {
			if (require_connected(&client) != 0)
				break;

			char topic_buf[512];

			read_line("Topic: ", topic_buf, sizeof topic_buf);
			if (topic_buf[0] == '\0') {
				printf("Topic required.\n");
				break;
			}

			uint32_t partition_index =
				read_u32_default("Partition: ", 0);
			uint64_t consume_offset =
				read_u64_default("Offset: ", 0);

			int status_code;
			struct network_response resp;

			if (broker_client_consume(&client,
									  topic_buf,
									  partition_index,
									  consume_offset,
									  &status_code,
									  &resp) != 0) {
				fprintf(stderr, "transport error\n");
				break;
			}

			if (status_code != 0) {
				printf("consume failed status=%d\n", status_code);
				network_response_deinit(&resp);
				break;
			}

			struct record parsed;
			memset(&parsed, 0, sizeof(parsed));

			if (record_deserialize(
					&parsed, resp.body, resp.body_length) < 0) {
				printf("invalid record in response body\n");
			} else {
				printf("timestamp=%" PRIu64 " offset=%" PRIu64 " crc=%08x "
					   "len=%u\nvalue: ",
					   parsed.header.timestamp,
					   parsed.header.offset,
					   parsed.header.crc,
					   parsed.header.value_length);
				if (parsed.header.value_length > 0 && parsed.value)
					fwrite(parsed.value,
						   1,
						   parsed.header.value_length,
						   stdout);
				putchar('\n');
				record_destroy(&parsed);
			}

			network_response_deinit(&resp);
			break;
		}
		case 5:
			broker_client_close(&client);
			printf("Disconnected.\n");
			break;
		case 6:
			broker_client_close(&client);
			return 0;
		default:
			printf("Unknown choice.\n");
		}
	}

	broker_client_close(&client);
	return 0;
}
