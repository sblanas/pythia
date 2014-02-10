dnl Process this file with autoconf to produce a configure script.
AC_INIT(libconfig.c)
AM_INIT_AUTOMAKE(libconfig, 1.2)
AM_CONFIG_HEADER(ac_config.h)

dnl Checks for programs.
dnl AC_PROG_INSTALL
AC_PROG_CC
AC_PROG_RANLIB
AC_LIBTOOL_WIN32_DLL
AC_PROG_LIBTOOL

AM_PROG_LEX
if test Z"$LEX" != Zflex; then
cat <<EOF
*******************************************************************
You do not seem to have flex. While flex is not required to build
libconfig, you may need it to regenerate the lexer if you change
the scanner.l file. Flex 2.5.33 or newer is recommended.
*******************************************************************
EOF
fi

AC_PROG_YACC
if test Z"$YACC" != "Zbison -y"; then
cat <<EOF
*******************************************************************
You do not seem to have bison. While bison is not required to build
libconfig, you may need it to regenerate the parser if you change
the grammar.y file.
*******************************************************************
EOF
fi

AC_MSG_CHECKING([for compiler switch to enable full warnings]);

dnl Checks for libraries

dnl Enable warnings, if we can determine an appropriate switch...
case "${CC}" in

gcc)
	warn_sw="-Wall";;
*)
	warn_sw="";;

esac

if test -n "${warn_sw}";
then
	CFLAGS="${CFLAGS} ${warn_sw}";
	CXXFLAGS="${CXXFLAGS} ${warn_sw}";
	AC_MSG_RESULT(${warn_sw})
else
	AC_MSG_RESULT([(cannot determine)])
fi;
	
dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS(unistd.h stdint.h)

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

dnl Package options

AC_ARG_ENABLE(cxx,
AS_HELP_STRING([--disable-cxx], [Disable building of the C++ library]),
[docxx=no],
[
AC_PROG_CXX
docxx=yes
]
)

AM_CONDITIONAL(BUILDCXX, test x$docxx = xyes)
 
dnl Checks for library functions.
 
AC_OUTPUT(
	Makefile
	doc/Makefile
	samples/Makefile
	samples/c/Makefile
	samples/c++/Makefile
	libconfig.pc
	libconfig++.pc
	libconfig.spec
	)
