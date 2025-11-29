TARGET = main
SRCDIR = src
LIBS = lib
CFLAGS = -g -fPIC -I include

TARGET_SRC = $(SRCDIR)/main.c
TARGET_OBJ = $(TARGET_SRC:.c=.o)

SRCS_FOR_LIB = $(SRCDIR)/bptree/bptree.c \
               $(SRCDIR)/bptree/bptree_utils.c \
               $(SRCDIR)/bptree/bptree_insert.c \
               $(SRCDIR)/bptree/bptree_delete.c \
               $(SRCDIR)/bptree/bptree_find.c \
               $(SRCDIR)/db_api.c \
               $(SRCDIR)/file.c

OBJS_FOR_LIB = $(SRCS_FOR_LIB:.c=.o)

.PHONY: all clean static_library library

all: static_library $(TARGET)

# Compile .o from .c
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Link main with static library
$(TARGET): $(TARGET_OBJ) $(OBJS_FOR_LIB)
	$(CC) $(CFLAGS) -o $@ $(TARGET_OBJ) -L $(LIBS) -lbpt

# Create static library
static_library: $(OBJS_FOR_LIB)
	ar cr $(LIBS)/libbpt.a $(OBJS_FOR_LIB)

clean:
	rm -f $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)/libbpt.*
