
lib_LTLIBRARIES = libconfig.la

if BUILDCXX
lib_LTLIBRARIES += libconfig++.la
endif

# Versioning rules ( C:R:A )
# 1. Start with version 0:0:0.
# 2. If the library source code has changed at all, incremement R.
# 3. If any interfaces have been added, removed, or changed, increment C and
#    set R to 0.
# 4. If any interfaces have been added, increment A.
# 5. If any interfaces have been removed, set A to 0.
# For more info see page 27 of the GNU Libtool Manual.

VERINFO = -version-info 5:0:0

PARSER_PREFIX = libconfig_yy

libconfig_la_LDFLAGS = $(VERINFO) -no-undefined

libconfig___la_LDFLAGS = $(VERINFO) -no-undefined

libsrc = libconfig.c scanner.l grammar.y private.h
libinc = libconfig.h

libsrc_cpp =  $(libsrc) libconfigcpp.c++
libinc_cpp =  $(libinc) libconfig.h++

BUILT_SOURCES = scanner.c scanner.h grammar.c grammar.h

libconfig_la_SOURCES = $(libsrc)

libconfig___la_SOURCES = $(libsrc_cpp)

libcflags = -D_REENTRANT

libconfig_la_CFLAGS = $(libcflags)
libconfig_la_CPPFLAGS = -DPARSER_PREFIX=\"$(PARSER_PREFIX)\"

libconfig___la_CFLAGS = $(libcflags)
libconfig___la_CPPFLAGS = -DPARSER_PREFIX=\"$(PARSER_PREFIX)\"

include_HEADERS = $(libinc)

if BUILDCXX
include_HEADERS += $(libinc_cpp)
endif

AM_YFLAGS = -d -p $(PARSER_PREFIX)

AM_LFLAGS = --header-file=scanner.h --prefix=$(PARSER_PREFIX)

EXTRA_DIST = \
	$(BUILT_SOURCES) \
	libconfig++.vcproj \
	libconfig++_stub.vcproj \
	libconfig.vcproj \
	libconfig_stub.vcproj \
	libconfig.sln \
	test.cfg \
	TODO \
	debian/changelog \
	debian/compat \
	debian/control \
	debian/copyright \
	debian/libconfigduo-dev.docs \
	debian/libconfigduo-dev.install \
	debian/libconfigduo.install \
	debian/rules \
	libconfig.spec

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libconfig.pc

if BUILDCXX
pkgconfig_DATA += libconfig++.pc
endif

SUBDIRS = . samples doc

.PHONY: dist-rpm

dist-rpm: distcheck
	rpmbuild -ta $(distdir).tar.gz
