CC ?= cc
CFLAGS ?= -O2 -fPIC -I../libvectorbase/include

TEST_SRCS = $(wildcard test_*.c)
TEST_EXES = $(TEST_SRCS:.c=)

.PHONY: all clean test

all: $(TEST_EXES)

test_%: test_%.c
	$(CC) $(CFLAGS) -o $@ $< -static -L../libvectorbase/lib ../libvectorbase/lib/libvectorbase.a

clean:
	rm -f $(TEST_EXES) *.o

test: $(TEST_EXES)
	@for test in $(TEST_EXES); do \
		echo "\n========================================"; \
		echo "Running $$test..."; \
		echo "========================================"; \
		./$$test || exit 1; \
	done