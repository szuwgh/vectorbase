CC     ?= cc
CFLAGS ?= -O2 -I ../src
LDLIBS  = ../src/libvectorbase.a -lm -lpthread

TEST_SRCS = $(wildcard test_*.c)
TEST_EXES = $(TEST_SRCS:.c=)

.PHONY: all clean test

all: $(TEST_EXES)

test_%: test_%.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)


clean:
	rm -f $(TEST_EXES) *.o

test: $(TEST_EXES)
	@failed=0; \
	for t in $(TEST_EXES); do \
		echo ""; \
		echo "========================================"; \
		echo "Running $$t ..."; \
		echo "========================================"; \
		./$$t || failed=$$((failed + 1)); \
	done; \
	echo ""; \
	if [ $$failed -eq 0 ]; then \
		echo "All tests passed."; \
	else \
		echo "$$failed test(s) FAILED."; \
		exit 1; \
	fi
