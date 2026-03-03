## Variables overridden by parent Makefile or `configure`
CC ?= cc
CFLAGS ?= -O2 -fPIC -I.
PREFIX ?= /usr/local
LIBDIR ?= $(PREFIX)/lib
INCLUDEDIR ?= $(PREFIX)/include

SRCS = $(wildcard *.c)
OBJS = $(SRCS:.c=.o)

LIBNAME = vectorbase
STATIC_LIB = lib$(LIBNAME).a
SHARED_LIB = lib$(LIBNAME).so

.PHONY: all clean install uninstall

all: $(STATIC_LIB) $(SHARED_LIB)

$(STATIC_LIB): $(OBJS)
	ar rcs $@ $(OBJS)

$(SHARED_LIB): CFLAGS += -fPIC
$(SHARED_LIB): $(OBJS)
	$(CC) -shared -o $@ $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(STATIC_LIB) $(SHARED_LIB)

install: all
	@echo "Installing to $(DESTDIR)$(LIBDIR) and $(DESTDIR)$(INCLUDEDIR)"
	install -d "$(DESTDIR)$(LIBDIR)"
	install -d "$(DESTDIR)$(INCLUDEDIR)"
	install -m 644 $(STATIC_LIB) "$(DESTDIR)$(LIBDIR)/"
	install -m 755 $(SHARED_LIB) "$(DESTDIR)$(LIBDIR)/"
	# install headers
	for h in *.h; do \
		if [ -f "$$h" ]; then install -m 644 "$$h" "$(DESTDIR)$(INCLUDEDIR)/"; fi; \
	done

uninstall:
	-rm -f "$(DESTDIR)$(LIBDIR)/$(STATIC_LIB)"
	-rm -f "$(DESTDIR)$(LIBDIR)/$(SHARED_LIB)"
	-for h in *.h; do \
		if [ -f "$(DESTDIR)$(INCLUDEDIR)/$$h" ]; then rm -f "$(DESTDIR)$(INCLUDEDIR)/$$h"; fi; \
	done
