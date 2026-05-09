# outputs live under ./build — use: make broker | make broker_cli | make lib
BUILD_DIR ?= build
OBJDIR = $(BUILD_DIR)/obj

CC = cc
INCLUDES = -Iinclude
CFLAGS = -std=c11 -D_DEFAULT_SOURCE -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -O2 \
	$(INCLUDES)
LDFLAGS = -lz
RANLIB = ranlib

LIB_A = $(BUILD_DIR)/libmessagebroker.a

OBJS_SHARED = $(OBJDIR)/network.o $(OBJDIR)/record.o
OBJS_LIB = $(OBJDIR)/client.o $(OBJDIR)/client_config.o $(OBJS_SHARED)
OBJS_BROKER = $(OBJDIR)/main.o $(OBJDIR)/broker.o $(OBJDIR)/broker_config.o \
	$(OBJDIR)/topic.o \
	$(OBJDIR)/partition.o $(OBJDIR)/segment.o $(OBJS_SHARED)

BROKER_BIN = $(BUILD_DIR)/broker
BROKER_CLI_BIN = $(BUILD_DIR)/broker_cli
CHAT_CLIENT_BIN = $(BUILD_DIR)/chat_client

OBJS_CHAT = $(OBJDIR)/chat_client.o

.PHONY: all broker broker_cli chat_client lib clean help

help:
	@echo "Targets (artifacts in $(BUILD_DIR)/):"
	@echo "  make broker       — daemon binary"
	@echo "  make broker_cli   — interactive CLI client"
	@echo "  make chat_client  — group chat sample"
	@echo "  make lib          — static library ($(notdir $(LIB_A)))"
	@echo "  make all          — broker + broker_cli + chat_client + lib"
	@echo "  make clean        — remove $(BUILD_DIR)/"

all: broker broker_cli chat_client lib

broker: $(BROKER_BIN)

broker_cli: $(BROKER_CLI_BIN)

chat_client: $(CHAT_CLIENT_BIN)

lib: $(LIB_A)

$(BUILD_DIR) $(OBJDIR):
	mkdir -p $@

$(OBJDIR)/%.o: src/%.c | $(OBJDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB_A): $(OBJS_LIB) | $(BUILD_DIR)
	$(AR) rcs $@ $(OBJS_LIB)
	$(RANLIB) $@

$(BROKER_BIN): $(OBJS_BROKER) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJS_BROKER) $(LDFLAGS)

# link CLI against the archive so symbols stay unified with dependents of the library.
$(BROKER_CLI_BIN): $(OBJDIR)/cli.o $(LIB_A) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJDIR)/cli.o $(LIB_A) $(LDFLAGS)

$(CHAT_CLIENT_BIN): $(OBJS_CHAT) $(LIB_A) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJS_CHAT) $(LIB_A) $(LDFLAGS) -lpthread

clean:
	rm -rf $(BUILD_DIR)
