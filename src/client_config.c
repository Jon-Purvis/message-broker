#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "../include/client_config.h"

static void broker_client_settings_free_owned_string(char **slot)
{
	if (!slot || !*slot)
		return;
	free(*slot);
	*slot = NULL;
}

static char *broker_client_settings_trim_inplace(char *text)
{
	char *end;

	if (!text)
		return text;
	while (*text != '\0' && isspace((unsigned char)*text))
		text++;
	if (*text == '\0')
		return text;
	end = text + strlen(text);
	while (end > text && isspace((unsigned char)end[-1]))
		--end;
	*end = '\0';
	return text;
}

static int broker_client_settings_parse_listen_port_string(
	const char *value_text,
	uint16_t *listen_port_output)
{
	char *invalid_character_pointer = NULL;
	unsigned long numeric_value_parsed;

	if (!value_text || !listen_port_output || value_text[0] == '\0')
		return -1;

	errno = 0;
	numeric_value_parsed =
		strtoul(value_text, &invalid_character_pointer, 10);
	if (errno != 0 ||
	    invalid_character_pointer == value_text ||
	    *invalid_character_pointer != '\0')
		return -1;
	if (numeric_value_parsed == 0UL ||
	    numeric_value_parsed > 65535UL)
		return -1;
	*listen_port_output = (uint16_t)numeric_value_parsed;
	return 0;
}

static int broker_client_settings_apply_line(struct broker_client_settings *settings,
					     char *line_buffer)
{
	char *equals_separator;
	char *key_token;
	char *value_token;

	if (!settings || !line_buffer)
		return -1;
	line_buffer = broker_client_settings_trim_inplace(line_buffer);
	if (line_buffer[0] == '\0' || line_buffer[0] == '#')
		return 0;

	equals_separator = strchr(line_buffer, '=');
	if (equals_separator == NULL)
		return 0;
	*equals_separator = '\0';
	key_token = broker_client_settings_trim_inplace(line_buffer);
	value_token =
		broker_client_settings_trim_inplace(equals_separator + 1);

	if (key_token[0] == '\0')
		return -1;

	if (strcasecmp(key_token, "host") == 0 ||
	    strcasecmp(key_token, "broker_host") == 0) {
		broker_client_settings_free_owned_string(&settings->broker_host);
		if (value_token[0] != '\0') {
			settings->broker_host = strdup(value_token);
			if (!settings->broker_host)
				return -1;
		}
		return 0;
	}

	if (strcasecmp(key_token, "hosts") == 0 ||
	    strcasecmp(key_token, "broker_hosts") == 0) {
		broker_client_settings_free_owned_string(&settings->broker_hosts);
		if (value_token[0] != '\0') {
			settings->broker_hosts = strdup(value_token);
			if (!settings->broker_hosts)
				return -1;
		}
		return 0;
	}

	if (strcasecmp(key_token, "port") == 0 ||
	    strcasecmp(key_token, "listen_port") == 0) {
		if (broker_client_settings_parse_listen_port_string(
			    value_token, &settings->broker_port) != 0)
			return -1;
		return 0;
	}

	return 0;
}

void broker_client_settings_set_defaults(
	struct broker_client_settings *settings)
{
	if (!settings)
		return;
	memset(settings, 0, sizeof(*settings));
	settings->broker_host = strdup("127.0.0.1");
	settings->broker_port = 3490;
}

void broker_client_settings_destroy(struct broker_client_settings *settings)
{
	if (!settings)
		return;
	broker_client_settings_free_owned_string(&settings->broker_host);
	broker_client_settings_free_owned_string(&settings->broker_hosts);
	memset(settings, 0, sizeof(*settings));
}

int broker_client_settings_merge_file(
	const char *path,
	struct broker_client_settings *settings)
{
	FILE *input_stream;
	char line_buffer[4096];

	if (!path || !settings)
		return -1;

	input_stream = fopen(path, "r");
	if (!input_stream) {
		if (errno == ENOENT)
			return -2;
		return -3;
	}

	while (fgets(line_buffer, sizeof line_buffer, input_stream) != NULL) {
		if (broker_client_settings_apply_line(settings, line_buffer) !=
		    0) {
			fclose(input_stream);
			return -1;
		}
	}
	if (ferror(input_stream)) {
		fclose(input_stream);
		return -3;
	}

	fclose(input_stream);
	return 0;
}

int broker_client_connect_with_settings(
	struct broker_client *client,
	const struct broker_client_settings *settings)
{
	const char *single_broker_host;

	if (!client || !settings)
		return -1;

	if (settings->broker_hosts && settings->broker_hosts[0] != '\0')
		return broker_client_connect_hosts(client,
						   settings->broker_hosts,
						   settings->broker_port);

	single_broker_host =
		(settings->broker_host && settings->broker_host[0] != '\0')
			? settings->broker_host
			: "127.0.0.1";

	return broker_client_connect(client,
				     single_broker_host,
				     settings->broker_port);
}

int broker_client_connect_via_config_file(struct broker_client *client,
					  const char *config_path_option)
{
	struct broker_client_settings loaded_settings;
	const int caller_supplied_explicit_path =
		config_path_option != NULL;
	const char *path_used_for_merge =
		caller_supplied_explicit_path ? config_path_option : "broker.conf";
	int merge_status;
	int tcp_connect_outcome;

	broker_client_settings_set_defaults(&loaded_settings);
	merge_status = broker_client_settings_merge_file(path_used_for_merge,
							 &loaded_settings);
	if (merge_status == -1 || merge_status == -3)
		goto fail_after_defaults;
	if (merge_status == -2 && caller_supplied_explicit_path)
		goto fail_after_defaults;

	tcp_connect_outcome =
		broker_client_connect_with_settings(client, &loaded_settings);
	broker_client_settings_destroy(&loaded_settings);
	return tcp_connect_outcome;

fail_after_defaults:
	broker_client_settings_destroy(&loaded_settings);
	return -1;
}
