# Compiler and flags
CC = clang
# -g for debug
CFLAGS = -I./include -I./include/util -I./include/faciledb -I./include/index -Wall -g

# Source and target directories
SRCDIR = src
OBJDIR = bin
INCLUDEDIR = include

# Main executable target name
TARGET = $(OBJDIR)/FacileDB

# Test executable target names
TEST_INDEX_TARGET = $(OBJDIR)/Test_Index
TEST_FACILEDB_TARGET = $(OBJDIR)/Test_Faciledb

# Source files for main target
SRC_ALL = $(wildcard $(SRCDIR)/*.c) $(wildcard $(SRCDIR)/util/*.c)
# filter out unfinished main
SRC = $(filter-out $(SRCDIR)/main.c, $(SRC_ALL))

# Object files for main target
OBJ = $(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.o, $(SRC))
OBJ_TEST_INDEX = $(filter-out $(OBJDIR)/index.o, $(OBJ))
OBJ_TEST_FACILEDB = $(filter-out $(OBJDIR)/faciledb.o, $(OBJ))


# Default target to build main executable
all: $(OBJDIR) $(TARGET)

# Main executable
$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $(TARGET)

# Test target to compile test_main.c and test_db_main.c
test: $(OBJDIR) $(OBJ) $(TEST_INDEX_TARGET) $(TEST_FACILEDB_TARGET)

# Test executable
$(TEST_INDEX_TARGET): $(SRCDIR)/test/test_index_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_INDEX) -I./src $(SRCDIR)/test/test_index_main.c -o $(TEST_INDEX_TARGET)

$(TEST_FACILEDB_TARGET): $(SRCDIR)/test/test_faciledb_main.c
	$(CC) $(CFLAGS) $(OBJ_TEST_FACILEDB) -I./src $(SRCDIR)/test/test_faciledb_main.c -o $(TEST_FACILEDB_TARGET)

# Ensure bin directory exists
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Compile .c files to .o files, creating directories if needed
$(OBJDIR)/%.o: $(SRCDIR)/%.c | $(OBJDIR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up all build artifacts
clean:
	rm -rf $(OBJDIR)/*.o $(OBJDIR)/*/*.o $(TARGET) $(TEST_INDEX_TARGET) $(TEST_FACILEDB_TARGET)

# Phony targets
.PHONY: all test clean