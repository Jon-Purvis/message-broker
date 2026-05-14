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

	if (strcasecmp(key_token, "brokers") == 0 ||
	    strcasecmp(key_token, "broker") == 0) {
		broker_client_settings_free_owned_string(&settings->broker_endpoints);
		if (value_token[0] != '\0') {
			settings->broker_endpoints = strdup(value_token);
			if (!settings->broker_endpoints)
				return -1;
		}
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
	settings->broker_endpoints = strdup("127.0.0.1:3490");
	if (!settings->broker_endpoints)
		return;
}

void broker_client_settings_destroy(struct broker_client_settings *settings)
{
	if (!settings)
		return;
	broker_client_settings_free_owned_string(&settings->broker_endpoints);
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
	if (!client || !settings)
		return -1;
	if (!settings->broker_endpoints ||
	    settings->broker_endpoints[0] == '\0')
		return -1;
	return broker_client_connect_hosts(client, settings->broker_endpoints);
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
	if (!loaded_settings.broker_endpoints)
		return -1;
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
