CC = cc
CFLAGS = -std=c11 -Wall -Wextra -Werror -O2
LDFLAGS = -lz

.PHONY: all clean

all: broker

broker:
	$(CC) $(CFLAGS) -o broker \
		src/main.c \
		src/broker.c \
		src/network.c \
		src/topic.c \
		src/partition.c \
		src/segment.c \
		src/record.c \
		$(LDFLAGS)

clean:
	rm -f broker
