CC       ?= cc
CFLAGS   ?= -O2 -Wall -Wextra

# Installed library paths (produced by `make install` from project root)
LIBDIR     = ../libvectorbase/lib
INCLUDEDIR = ../libvectorbase/include
STATIC_LIB = $(LIBDIR)/libvectorbase.a

CFLAGS  += -I$(INCLUDEDIR)
LDFLAGS  = -lm

TARGET = example_datatable

.PHONY: all clean run lib

all: $(TARGET)

# Build and install the static library first
lib:
	$(MAKE) -C .. install

# Link example against the static library; rebuild lib if needed
$(TARGET): example_datatable.c | lib
	$(CC) $(CFLAGS) -o $@ $< $(STATIC_LIB) $(LDFLAGS)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
