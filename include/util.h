#ifndef UTIL_H
#define UTIL_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>

/*
 * ----------------------------------------------------------------------------
 * ENDIANNESS HELPERS
 * ----------------------------------------------------------------------------
 */
static inline int is_big_endian(void)
{
	const uint16_t x = 1;
	const uint8_t *p = (const uint8_t *)&x;
	return *p == 0;
}

static inline uint64_t broker_htonll(uint64_t val)
{
	if (is_big_endian())
		return val;

	// grab the low 32 bits, swap them, and move to the top
	uint64_t high_part = (uint64_t)htonl(val & 0xFFFFFFFFLL) << 32;

	// grab the high 32 bits, swap them, and keep them at the bottom
	uint64_t low_part = (uint64_t)htonl(val >> 32);

	// put together
	return high_part | low_part;
}

static inline uint64_t broker_ntohll(uint64_t val)
{
	return broker_htonll(val);
}

/*
 * ----------------------------------------------------------------------------
 * SERIALIZATION HELPERS
 * ----------------------------------------------------------------------------
 */
static inline void pack_u16(uint8_t **p, uint16_t val)
{
	uint16_t net = htons(val);
	memcpy(*p, &net, 2);
	*p += 2;
}

static inline void pack_u32(uint8_t **p, uint32_t val)
{
	uint32_t net = htonl(val);
	memcpy(*p, &net, 4);
	*p += 4;
}

static inline void pack_u64(uint8_t **p, uint64_t val)
{
	uint64_t net = broker_htonll(val);
	memcpy(*p, &net, 8);
	*p += 8;
}

static inline uint16_t unpack_u16(const uint8_t **p)
{
	uint16_t net;
	memcpy(&net, *p, 2);
	*p += 2;
	return ntohs(net);
}

static inline uint32_t unpack_u32(const uint8_t **p)
{
	uint32_t net;
	memcpy(&net, *p, 4);
	*p += 4;
	return ntohl(net);
}

static inline uint64_t unpack_u64(const uint8_t **p)
{
	uint64_t net;
	memcpy(&net, *p, 8);
	*p += 8;
	return broker_ntohll(net);
}

static inline char *path_join(const char *base, const char *sub)
{
	int length = snprintf(NULL, 0, "%s/%s", base, sub);
	if (length < 0)
		return NULL;

	char *result = (char *)malloc(length + 1);
	if (result)
		snprintf(result, length + 1, "%s/%s", base, sub);

	return result;
}

#endif
