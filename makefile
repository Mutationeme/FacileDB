# Compiler and flags
CC = clang
CFLAGS = -I./include -I./include/util -I./include/faciledb -I./include/index -I./include/mema -Wall -g
LDFLAGS = -lz -pthread

# Source and target directories
SRCDIR = src
OBJDIR = bin

# Main library name
LIB_NAME = $(OBJDIR)/libfaciledb.a

# Test executable target names
TEST_INDEX_TARGET = $(OBJDIR)/Test_Index
TEST_FACILEDB_TARGET = $(OBJDIR)/Test_Faciledb
TEST_HASH_TARGET = $(OBJDIR)/Test_Hash
TEST_CRC_TARGET = $(OBJDIR)/Test_Crc

# Collect all source files (including subdirectories)
SRC = $(wildcard $(SRCDIR)/*.c) \
      $(wildcard $(SRCDIR)/util/*.c) \
      $(wildcard $(SRCDIR)/faciledb/*.c) \
      $(wildcard $(SRCDIR)/index/*.c) \
	  $(wildcard $(SRCDIR)/mema/*.c)

# Object files for main target
OBJ = $(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.o, $(SRC))
OBJ_TEST_INDEX = $(filter-out $(OBJDIR)/index/index.o, $(OBJ))
OBJ_TEST_FACILEDB = $(filter-out $(OBJDIR)/faciledb/faciledb.o $(OBJDIR)/faciledb/faciledb_api.o $(OBJDIR)/faciledb/faciledb_insert.o $(OBJDIR)/faciledb/faciledb_search.o $(OBJDIR)/faciledb/faciledb_delete.o, $(OBJ))
OBJ_TEST_HASH = $(filter-out $(OBJDIR)/util/hash.o, $(OBJ))
OBJ_TEST_CRC = $(filter-out $(OBJDIR)/util/crc.o, $(OBJ))

# Default library
all: $(OBJDIR) $(LIB_NAME)

# Build static library
$(LIB_NAME): $(OBJ)
	ar rcs $@ $^

# Test targets
test: $(OBJDIR) $(OBJ) $(TEST_INDEX_TARGET) $(TEST_FACILEDB_TARGET) $(TEST_HASH_TARGET) $(TEST_CRC_TARGET)

$(TEST_INDEX_TARGET): $(SRCDIR)/test/test_index_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_INDEX) -I$(SRCDIR)/index $(LDFLAGS) $(SRCDIR)/test/test_index_main.c -o $(TEST_INDEX_TARGET)

$(TEST_FACILEDB_TARGET): $(SRCDIR)/test/test_faciledb_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_FACILEDB) -I$(SRCDIR)/faciledb $(LDFLAGS) $(SRCDIR)/test/test_faciledb_main.c -o $(TEST_FACILEDB_TARGET)

$(TEST_HASH_TARGET): $(SRCDIR)/test/test_hash_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_HASH) -I$(SRCDIR)/util $(LDFLAGS) $(SRCDIR)/test/test_hash_main.c -o $(TEST_HASH_TARGET)

$(TEST_CRC_TARGET): $(SRCDIR)/test/test_crc_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_CRC) -I$(SRCDIR)/util $(LDFLAGS) $(SRCDIR)/test/test_crc_main.c -o $(TEST_CRC_TARGET)

# Ensure bin directory exists
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Compile .c -> .o (auto-create subdirectories in bin/)
$(OBJDIR)/%.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up all build artifacts
clean:
	rm -rf $(OBJDIR)

# Phony targets
.PHONY: all test clean