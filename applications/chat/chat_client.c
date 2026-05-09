/*
 * Group-chat client built on top of the message-broker library.
 *
 * Replaces the previous hand-rolled TCP server + custom protocol + threaded
 * fan-out: each chat group is now a broker topic, sending is a produce, and
 * receiving is a per-group consumer thread that polls by offset.
 */

#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../../include/client.h"
#include "../../include/client_config.h"
#include "../../include/network.h"
#include "../../include/record.h"

#define DEFAULT_BROKER_HOST "127.0.0.1"
#define DEFAULT_BROKER_PORT 3490
#define DEFAULT_GROUP_PARTITION_COUNT 1

#define MAX_DISPLAY_NAME_LENGTH 63
#define MAX_GROUP_NAME_LENGTH 63
#define MAX_MESSAGE_TEXT_LENGTH 512
#define MAX_JOINED_GROUPS 16

/* Polling interval is the user-perceived chat latency floor; 50 ms feels live
 * without thrashing the broker. */
#define CONSUMER_POLL_INTERVAL_MICROSECONDS (50 * 1000)

static const char *const default_group_names[] = {
	"CMPS",
	"CMPS_340",
	"CMPS_352",
};
static const size_t default_group_count =
	sizeof(default_group_names) / sizeof(default_group_names[0]);

static volatile sig_atomic_t global_stop_flag = 0;
/*
 * When set, consumer threads use broker_client_connect_hosts with this CSV
 * (same semantics as BROKER_HOSTS but sourced from broker.conf / chat-client.conf).
 * Owned for process lifetime; freed before main returns.
 */
static char *chat_failover_hosts_from_config_file = NULL;

static void chat_copy_first_host_from_csv(char *destination_buffer,
					  size_t destination_length,
					  const char *comma_separated_hosts)
{
	size_t segment_length;
	size_t tail;

	if (!destination_buffer || destination_length == 0 ||
	    !comma_separated_hosts || comma_separated_hosts[0] == '\0')
		return;

	while (*comma_separated_hosts == ' ' || *comma_separated_hosts == '\t')
		comma_separated_hosts++;

	segment_length = strcspn(comma_separated_hosts, ",");
	if (segment_length >= destination_length)
		segment_length = destination_length - 1;
	memcpy(destination_buffer, comma_separated_hosts, segment_length);
	destination_buffer[segment_length] = '\0';

	tail = segment_length;
	while (tail > 0 &&
	       isspace((unsigned char)destination_buffer[tail - 1])) {
		destination_buffer[--tail] = '\0';
	}
}

struct chat_consumer_thread_arguments {
	char broker_host[256];
	uint16_t broker_port;
	char topic_name[MAX_GROUP_NAME_LENGTH + 1];
	char display_name[MAX_DISPLAY_NAME_LENGTH + 1];
};

static void chat_handle_sigint(int signal_number)
{
	(void)signal_number;
	global_stop_flag = 1;
}

static void chat_trim_trailing_newline(char *buffer)
{
	size_t length = strlen(buffer);
	while (length > 0 &&
		   (buffer[length - 1] == '\n' || buffer[length - 1] == '\r')) {
		buffer[--length] = '\0';
	}
}

static int chat_prompt_line(const char *prompt,
							char *buffer,
							size_t buffer_size)
{
	printf("%s", prompt);
	fflush(stdout);
	if (!fgets(buffer, (int)buffer_size, stdin))
		return -1;
	chat_trim_trailing_newline(buffer);
	return 0;
}

static int chat_is_known_group(const char *group_name)
{
	for (size_t i = 0; i < default_group_count; i++) {
		if (strcmp(default_group_names[i], group_name) == 0)
			return 1;
	}
	return 0;
}

static int chat_is_joined_group(
	const char joined_group_set[][MAX_GROUP_NAME_LENGTH + 1],
	size_t joined_group_count,
	const char *target_group)
{
	for (size_t i = 0; i < joined_group_count; i++) {
		if (strcmp(joined_group_set[i], target_group) == 0)
			return 1;
	}
	return 0;
}

static int chat_ensure_topics_exist(struct broker_client *producer_client)
{
	for (size_t i = 0; i < default_group_count; i++) {
		int broker_status = -1;
		if (broker_client_create_topic(producer_client,
									   default_group_names[i],
									   DEFAULT_GROUP_PARTITION_COUNT,
									   &broker_status) != 0) {
			fprintf(stderr,
					"create_topic transport error for %s\n",
					default_group_names[i]);
			return -1;
		}
		/* Non-zero means the topic already exists from a previous client run
		 * which is the expected steady state. */
	}
	return 0;
}

static void *chat_consumer_thread_main(void *raw_arguments)
{
	struct chat_consumer_thread_arguments *arguments =
		(struct chat_consumer_thread_arguments *)raw_arguments;

	struct broker_client consumer_client;
	uint64_t next_offset_to_read = 0;
	char own_message_prefix[MAX_DISPLAY_NAME_LENGTH + 3];
	int own_message_prefix_length = snprintf(own_message_prefix,
											 sizeof own_message_prefix,
											 "%s: ",
											 arguments->display_name);

	broker_client_init(&consumer_client);
	{
		const char *hosts_env_override = getenv("BROKER_HOSTS");

		if (hosts_env_override != NULL && hosts_env_override[0] != '\0') {
			if (broker_client_connect_hosts(
					&consumer_client,
					hosts_env_override,
					arguments->broker_port) != 0) {
				fprintf(stderr,
						"[%s] consumer failed (BROKER_HOSTS)\n",
						arguments->topic_name);
				free(arguments);
				return NULL;
			}
		} else if (chat_failover_hosts_from_config_file != NULL &&
			   chat_failover_hosts_from_config_file[0] != '\0') {
			if (broker_client_connect_hosts(
					&consumer_client,
					chat_failover_hosts_from_config_file,
					arguments->broker_port) != 0) {
				fprintf(stderr,
						"[%s] consumer failed (config hosts list)\n",
						arguments->topic_name);
				free(arguments);
				return NULL;
			}
		} else if (broker_client_connect(&consumer_client,
						 arguments->broker_host,
						 arguments->broker_port) != 0) {
			fprintf(stderr,
				"[%s] consumer failed to connect to broker\n",
				arguments->topic_name);
			free(arguments);
			return NULL;
		}
	}

	while (!global_stop_flag) {
		int broker_status_code = -1;
		struct network_response broker_response;

		if (broker_client_consume(&consumer_client,
								  arguments->topic_name,
								  0,
								  next_offset_to_read,
								  &broker_status_code,
								  &broker_response) != 0) {
			break;
		}

		if (broker_status_code != 0) {
			network_response_deinit(&broker_response);
			usleep(CONSUMER_POLL_INTERVAL_MICROSECONDS);
			continue;
		}

		struct record consumed_record;
		memset(&consumed_record, 0, sizeof(consumed_record));
		if (record_deserialize(&consumed_record,
							   broker_response.body,
							   broker_response.body_length) < 0) {
			network_response_deinit(&broker_response);
			continue;
		}

		int is_own_message = (consumed_record.header.value_length >=
							  (uint32_t)own_message_prefix_length) &&
			(memcmp(consumed_record.value,
					own_message_prefix,
					(size_t)own_message_prefix_length) == 0);

		if (!is_own_message && consumed_record.header.value_length > 0) {
			printf("\n[%s] %.*s\n> ",
				   arguments->topic_name,
				   (int)consumed_record.header.value_length,
				   (const char *)consumed_record.value);
			fflush(stdout);
		}

		next_offset_to_read = consumed_record.header.offset + 1;
		record_destroy(&consumed_record);
		network_response_deinit(&broker_response);
	}

	broker_client_close(&consumer_client);
	free(arguments);
	return NULL;
}

static int chat_start_consumer_thread_for_group(const char *broker_host,
												uint16_t broker_port,
												const char *topic_name,
												const char *display_name)
{
	struct chat_consumer_thread_arguments *arguments =
		malloc(sizeof(*arguments));
	if (!arguments)
		return -1;

	snprintf(arguments->broker_host,
			 sizeof arguments->broker_host,
			 "%s",
			 broker_host);
	arguments->broker_port = broker_port;
	snprintf(arguments->topic_name,
			 sizeof arguments->topic_name,
			 "%s",
			 topic_name);
	snprintf(arguments->display_name,
			 sizeof arguments->display_name,
			 "%s",
			 display_name);

	pthread_t thread_handle;
	if (pthread_create(&thread_handle,
					   NULL,
					   chat_consumer_thread_main,
					   arguments) != 0) {
		free(arguments);
		return -1;
	}
	pthread_detach(thread_handle);
	return 0;
}

static int chat_send_message_to_group(struct broker_client *producer_client,
									  const char *display_name,
									  const char *topic_name,
									  const char *message_text)
{
	char framed_value[MAX_DISPLAY_NAME_LENGTH + 3 + MAX_MESSAGE_TEXT_LENGTH + 1];
	int formatted_length = snprintf(framed_value,
									sizeof framed_value,
									"%s: %s",
									display_name,
									message_text);
	if (formatted_length < 0 ||
		formatted_length >= (int)sizeof framed_value) {
		return -1;
	}

	int broker_status_code = -1;
	uint32_t assigned_partition = 0;
	uint64_t assigned_offset = 0;

	if (broker_client_produce(producer_client,
							  topic_name,
							  display_name,
							  strlen(display_name),
							  framed_value,
							  (size_t)formatted_length,
							  &broker_status_code,
							  &assigned_partition,
							  &assigned_offset) != 0) {
		fprintf(stderr, "produce transport error\n");
		return -1;
	}
	if (broker_status_code != 0) {
		fprintf(stderr,
				"broker rejected produce (status=%d)\n",
				broker_status_code);
		return -1;
	}
	return 0;
}

static void chat_print_main_menu(void)
{
	printf("\n--- Menu ---\n");
	printf("1. Send message to group\n");
	printf("2. Join group\n");
	printf("3. Exit\n");
	printf("> ");
	fflush(stdout);
}

static int chat_read_menu_choice(void)
{
	char line[16];
	if (!fgets(line, sizeof line, stdin))
		return 0;
	return atoi(line);
}

static int chat_handle_send_choice(
	struct broker_client *producer_client,
	const char *display_name,
	const char joined_group_set[][MAX_GROUP_NAME_LENGTH + 1],
	size_t joined_group_count)
{
	char target_group[MAX_GROUP_NAME_LENGTH + 1];
	if (chat_prompt_line("Group (CMPS / CMPS_340 / CMPS_352): ",
						 target_group,
						 sizeof target_group) != 0) {
		return -1;
	}
	if (!chat_is_known_group(target_group)) {
		printf("unknown group\n");
		return 0;
	}
	/* Refuse produce-without-subscribe so the user never publishes into a
	 * topic whose replies their client cannot see. */
	if (!chat_is_joined_group(joined_group_set,
							  joined_group_count,
							  target_group)) {
		printf("not a member of %s; join it first (option 2)\n",
			   target_group);
		return 0;
	}

	char message_text[MAX_MESSAGE_TEXT_LENGTH + 1];
	if (chat_prompt_line("Message: ", message_text, sizeof message_text) != 0)
		return -1;
	if (message_text[0] == '\0') {
		printf("empty message\n");
		return 0;
	}
	return chat_send_message_to_group(producer_client,
									  display_name,
									  target_group,
									  message_text);
}

static int chat_handle_join_choice(const char *broker_host,
								   uint16_t broker_port,
								   const char *display_name,
								   char joined_group_set[][MAX_GROUP_NAME_LENGTH + 1],
								   size_t *joined_group_count_in_out)
{
	char target_group[MAX_GROUP_NAME_LENGTH + 1];
	if (chat_prompt_line("Group to join (CMPS_340 / CMPS_352): ",
						 target_group,
						 sizeof target_group) != 0) {
		return -1;
	}
	if (!chat_is_known_group(target_group)) {
		printf("unknown group\n");
		return 0;
	}
	if (chat_is_joined_group(joined_group_set,
							 *joined_group_count_in_out,
							 target_group)) {
		printf("already joined %s\n", target_group);
		return 0;
	}
	if (*joined_group_count_in_out >= MAX_JOINED_GROUPS) {
		printf("joined-group limit reached\n");
		return 0;
	}
	if (chat_start_consumer_thread_for_group(broker_host,
											 broker_port,
											 target_group,
											 display_name) != 0) {
		fprintf(stderr, "failed to start consumer for %s\n", target_group);
		return -1;
	}
	snprintf(joined_group_set[*joined_group_count_in_out],
			 MAX_GROUP_NAME_LENGTH + 1,
			 "%s",
			 target_group);
	(*joined_group_count_in_out)++;
	printf("joined %s\n", target_group);
	return 0;
}

int main(int argc, char *argv[])
{
	struct broker_client_settings connection_file_overlay;
	char broker_host[256];
	uint16_t broker_port;

	broker_client_settings_set_defaults(&connection_file_overlay);
	{
		int merge_broker_file_status =
			broker_client_settings_merge_file(
				"broker.conf", &connection_file_overlay);

		if (merge_broker_file_status == -1 ||
		    merge_broker_file_status == -3) {
			fprintf(stderr,
				"could not parse or read broker.conf\n");
			broker_client_settings_destroy(&connection_file_overlay);
			return 1;
		}
	}
	{
		int merge_chat_file_status =
			broker_client_settings_merge_file(
				"chat-client.conf",
				&connection_file_overlay);

		if (merge_chat_file_status == -1 ||
		    merge_chat_file_status == -3) {
			fprintf(stderr,
				"could not parse or read chat-client.conf\n");
			broker_client_settings_destroy(&connection_file_overlay);
			return 1;
		}
	}

	broker_port = connection_file_overlay.broker_port;

	if (argc >= 2) {
		snprintf(broker_host, sizeof broker_host, "%s", argv[1]);
	} else if (connection_file_overlay.broker_hosts != NULL &&
		   connection_file_overlay.broker_hosts[0] != '\0') {
		chat_copy_first_host_from_csv(
			broker_host,
			sizeof broker_host,
			connection_file_overlay.broker_hosts);
	} else if (connection_file_overlay.broker_host != NULL &&
		   connection_file_overlay.broker_host[0] != '\0') {
		snprintf(broker_host,
			 sizeof broker_host,
			 "%s",
			 connection_file_overlay.broker_host);
	} else {
		snprintf(broker_host,
			 sizeof broker_host,
			 "%s",
			 DEFAULT_BROKER_HOST);
	}

	if (argc >= 3)
		broker_port = (uint16_t)strtoul(argv[2], NULL, 10);

	/* broker_client_connect_hosts copies this list internally; strdup only to
	   survive destruction of overlay memory. */
	{
		char *hosts_csv_for_connections = NULL;

		if (connection_file_overlay.broker_hosts != NULL &&
		    connection_file_overlay.broker_hosts[0] != '\0') {
			hosts_csv_for_connections =
				strdup(connection_file_overlay.broker_hosts);
			if (hosts_csv_for_connections == NULL) {
				broker_client_settings_destroy(&connection_file_overlay);
				return 1;
			}
		}
		broker_client_settings_destroy(&connection_file_overlay);
		signal(SIGINT, chat_handle_sigint);

		char display_name[MAX_DISPLAY_NAME_LENGTH + 1];
		struct broker_client producer_client;

		const char *hosts_env_override = getenv("BROKER_HOSTS");

		if (chat_prompt_line("Enter your display name: ",
				     display_name,
				     sizeof display_name) != 0 ||
		    display_name[0] == '\0') {
			fprintf(stderr, "display name required\n");
			free(hosts_csv_for_connections);
			return 1;
		}

		broker_client_init(&producer_client);

		if (hosts_env_override != NULL && hosts_env_override[0] != '\0') {
			if (broker_client_connect_hosts(
				    &producer_client,
				    hosts_env_override,
				    broker_port) != 0) {
				fprintf(
					stderr,
					"could not connect to any broker in BROKER_HOSTS\n");
				free(hosts_csv_for_connections);
				broker_client_close(&producer_client);
				return 1;
			}
		} else if (hosts_csv_for_connections != NULL) {
			if (broker_client_connect_hosts(&producer_client,
							hosts_csv_for_connections,
							broker_port) != 0) {
				fprintf(stderr,
					"could not connect hosts from config "
					"file list\n");
				free(hosts_csv_for_connections);
				broker_client_close(&producer_client);
				return 1;
			}
			chat_failover_hosts_from_config_file =
				hosts_csv_for_connections;
			hosts_csv_for_connections = NULL;
		} else if (broker_client_connect(
				   &producer_client,
				   broker_host,
				   broker_port) != 0) {
			fprintf(stderr,
				"could not connect to broker at %s:%u\n",
				broker_host,
				(unsigned)broker_port);
			free(hosts_csv_for_connections);
			broker_client_close(&producer_client);
			return 1;
		}

		free(hosts_csv_for_connections);

		if (chat_ensure_topics_exist(&producer_client) != 0) {
			free(chat_failover_hosts_from_config_file);
			chat_failover_hosts_from_config_file = NULL;
			broker_client_close(&producer_client);
			return 1;
		}

		/* Every client auto-subscribes to the always-on CMPS group, mirroring the
		 * original server's behavior of placing newly-registered users in CMPS. */
		if (chat_start_consumer_thread_for_group(
			    broker_host, broker_port, "CMPS", display_name) !=
		    0) {
			fprintf(stderr, "failed to start consumer for CMPS\n");
			free(chat_failover_hosts_from_config_file);
			chat_failover_hosts_from_config_file = NULL;
			broker_client_close(&producer_client);
			return 1;
		}

		char joined_group_set[MAX_JOINED_GROUPS][MAX_GROUP_NAME_LENGTH +
							 1];
		size_t joined_group_count = 0;
		snprintf(joined_group_set[joined_group_count++],
			 MAX_GROUP_NAME_LENGTH + 1,
			 "%s",
			 "CMPS");

		while (!global_stop_flag) {
			chat_print_main_menu();
			int menu_choice = chat_read_menu_choice();

			switch (menu_choice) {
			case 1:
				(void)chat_handle_send_choice(&producer_client,
							      display_name,
							      joined_group_set,
							      joined_group_count);
				break;
			case 2:
				(void)chat_handle_join_choice(broker_host,
							      broker_port,
							      display_name,
							      joined_group_set,
							      &joined_group_count);
				break;
			case 3:
				global_stop_flag = 1;
				break;
			default:
				printf("invalid choice\n");
			}
		}

		broker_client_close(&producer_client);
		/* Detached consumer threads observe the stop flag on their next poll
		 * cycle; give them a moment to drain before the process exits. */
		usleep(CONSUMER_POLL_INTERVAL_MICROSECONDS * 4);
		free(chat_failover_hosts_from_config_file);
		chat_failover_hosts_from_config_file = NULL;
		return 0;
	}
}
