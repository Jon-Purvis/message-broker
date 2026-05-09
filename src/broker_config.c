#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "../include/broker_config.h"

static void broker_config_free_string_field(char **slot)
{
	if (!slot || !*slot)
		return;
	free(*slot);
	*slot = NULL;
}

static char *broker_config_trim_inplace(char *text)
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

static int broker_config_parse_bool(const char *value, int *out_bool)
{
	if (!value || !out_bool)
		return -1;

	if (strcasecmp(value, "true") == 0 || strcasecmp(value, "yes") == 0 ||
		strcasecmp(value, "on") == 0 || strcmp(value, "1") == 0) {
		*out_bool = 1;
		return 0;
	}
	if (strcasecmp(value, "false") == 0 || strcasecmp(value, "no") == 0 ||
		strcasecmp(value, "off") == 0 || strcmp(value, "0") == 0) {
		*out_bool = 0;
		return 0;
	}
	return -1;
}

static int broker_config_parse_u16_port(const char *value, uint16_t *out_port)
{
	char *end_pointer = NULL;
	unsigned long parsed;

	if (!value || !out_port || value[0] == '\0')
		return -1;
	errno = 0;
	parsed = strtoul(value, &end_pointer, 10);
	if (errno != 0 || end_pointer == value || *end_pointer != '\0')
		return -1;
	if (parsed == 0UL || parsed > 65535UL)
		return -1;
	*out_port = (uint16_t)parsed;
	return 0;
}

static int broker_config_parse_size(const char *value, size_t *out_size)
{
	char *end_pointer = NULL;
	unsigned long long parsed;

	if (!value || !out_size || value[0] == '\0')
		return -1;
	errno = 0;
	parsed = strtoull(value, &end_pointer, 10);
	if (errno != 0 || end_pointer == value || *end_pointer != '\0')
		return -1;
	if (parsed > (unsigned long long)SIZE_MAX)
		return -1;
	*out_size = (size_t)parsed;
	return 0;
}

static int broker_config_apply_line(struct broker_config *config, char *line)
{
	char *equals_sign;
	char *key;
	char *value;
	int bool_out;

	if (!config || !line)
		return -1;
	line = broker_config_trim_inplace(line);
	if (line[0] == '\0' || line[0] == '#')
		return 0;

	equals_sign = strchr(line, '=');
	if (equals_sign == NULL)
		return -1;
	*equals_sign = '\0';
	key = broker_config_trim_inplace(line);
	value = broker_config_trim_inplace(equals_sign + 1);

	if (key[0] == '\0')
		return -1;

	if (strcasecmp(key, "data_dir") == 0) {
		broker_config_free_string_field(&config->data_dir);
		if (value[0] != '\0') {
			config->data_dir = strdup(value);
			if (!config->data_dir)
				return -1;
		}
		return 0;
	}
	if (strcasecmp(key, "listen_port") == 0 || strcasecmp(key, "port") == 0) {
		if (broker_config_parse_u16_port(value, &config->listen_port) != 0)
			return -1;
		return 0;
	}
	if (strcasecmp(key, "replica_host") == 0) {
		broker_config_free_string_field(&config->replica_host);
		if (value[0] != '\0') {
			config->replica_host = strdup(value);
			if (!config->replica_host)
				return -1;
		}
		return 0;
	}
	if (strcasecmp(key, "replica_port") == 0) {
		char *end_pointer = NULL;
		unsigned long parsed;

		if (value[0] == '\0') {
			config->replica_port = 0;
			return 0;
		}
		errno = 0;
		parsed = strtoul(value, &end_pointer, 10);
		if (errno != 0 || end_pointer == value || *end_pointer != '\0')
			return -1;
		if (parsed > 65535UL)
			return -1;
		config->replica_port = (uint16_t)parsed;
		return 0;
	}
	if (strcasecmp(key, "max_request_frame_bytes") == 0) {
		if (broker_config_parse_size(value, &config->max_request_frame_bytes) !=
			0)
			return -1;
		if (config->max_request_frame_bytes < BROKER_CONFIG_MIN_FRAME_BYTES ||
			config->max_request_frame_bytes > BROKER_CONFIG_MAX_FRAME_BYTES)
			return -1;
		return 0;
	}
	if (strcasecmp(key, "log_topic_actions") == 0) {
		if (broker_config_parse_bool(value, &bool_out) != 0)
			return -1;
		config->log_topic_actions = bool_out;
		return 0;
	}
	if (strcasecmp(key, "log_client_io") == 0) {
		if (broker_config_parse_bool(value, &bool_out) != 0)
			return -1;
		config->log_client_io = bool_out;
		return 0;
	}

	return -1;
}

void broker_config_set_defaults(struct broker_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	config->data_dir = strdup("./data");
	config->listen_port = 3490;
	config->max_request_frame_bytes = 1024UL * 1024UL;
	config->log_topic_actions = 1;
	config->log_client_io = 1;
}

void broker_config_destroy(struct broker_config *config)
{
	if (!config)
		return;
	broker_config_free_string_field(&config->data_dir);
	broker_config_free_string_field(&config->replica_host);
	memset(config, 0, sizeof(*config));
}

int broker_config_merge_file(const char *path, struct broker_config *config)
{
	FILE *stream;
	char line_buffer[4096];

	if (!path || !config)
		return -1;

	stream = fopen(path, "r");
	if (!stream) {
		if (errno == ENOENT)
			return -2;
		return -3;
	}

	while (fgets(line_buffer, sizeof line_buffer, stream) != NULL) {
		if (broker_config_apply_line(config, line_buffer) != 0) {
			fclose(stream);
			return -1;
		}
	}
	if (ferror(stream)) {
		fclose(stream);
		return -3;
	}

	fclose(stream);
	return 0;
}
