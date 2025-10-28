# Compiler and flags
CC = clang
CFLAGS = -I./include -I./include/util -I./include/faciledb -I./include/index -Wall -g

# Source and target directories
SRCDIR = src
OBJDIR = bin

# Main library name
LIB_NAME = $(OBJDIR)/libfaciledb.a

# Test executable target names
TEST_INDEX_TARGET = $(OBJDIR)/Test_Index
TEST_FACILEDB_TARGET = $(OBJDIR)/Test_Faciledb

# Collect all source files (including subdirectories)
SRC = $(wildcard $(SRCDIR)/*.c) \
      $(wildcard $(SRCDIR)/util/*.c) \
      $(wildcard $(SRCDIR)/faciledb/*.c) \
      $(wildcard $(SRCDIR)/index/*.c)

# Filter out unfinished main
# SRC = $(filter-out $(SRCDIR)/main.c, $(SRC_ALL))

# Object files for main target
OBJ = $(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.o, $(SRC))
OBJ_TEST_INDEX = $(filter-out $(OBJDIR)/index/index.o, $(OBJ))
OBJ_TEST_FACILEDB = $(filter-out $(OBJDIR)/faciledb/faciledb.o, $(OBJ))

# Default library
all: $(OBJDIR) $(LIB_NAME)

# Build static library
$(LIB_NAME): $(OBJ)
	ar rcs $@ $^

# Test targets
test: $(OBJDIR) $(OBJ) $(TEST_INDEX_TARGET) $(TEST_FACILEDB_TARGET)

$(TEST_INDEX_TARGET): $(SRCDIR)/test/test_index_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_INDEX) -I$(SRCDIR)/index -pthread $(SRCDIR)/test/test_index_main.c -o $(TEST_INDEX_TARGET)

$(TEST_FACILEDB_TARGET): $(SRCDIR)/test/test_faciledb_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_FACILEDB) -I$(SRCDIR)/faciledb -pthread $(SRCDIR)/test/test_faciledb_main.c -o $(TEST_FACILEDB_TARGET)

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