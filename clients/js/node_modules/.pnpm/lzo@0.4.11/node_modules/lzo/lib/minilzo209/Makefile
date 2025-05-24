#
# a very simple Makefile for miniLZO
#
# Copyright (C) 1996-2015 Markus F.X.J. Oberhumer
#

PROGRAM = testmini
SOURCES = testmini.c minilzo.c

default:
	@echo ""
	@echo "Welcome to miniLZO. Please choose one of the following 'make' targets:"
	@echo ""
	@echo "    gcc:   gcc"
	@echo "    unix:  hpux hpux9"
	@echo "    win32: win32-bc win32-cygwin win32-dm win32-lccwin32"
	@echo "           win32-intelc win32-mingw win32-vc win32-watcomc"
	@echo "    dos32: dos32-djgpp2 dos32-wc"
	@echo ""


# Make sure that minilzo.h, lzoconf.h and lzodefs.h are in the
# current dircectory. Otherwise you may want to adjust CPPFLAGS.
CPPFLAGS = -I. -I../include/lzo

GCC_CFLAGS = -s -Wall -O2 -fomit-frame-pointer


#
# gcc (generic)
#

gcc:
	gcc $(CPPFLAGS) $(GCC_CFLAGS) -o $(PROGRAM) $(SOURCES)

cc:
	cc $(CPPFLAGS) -o $(PROGRAM) $(SOURCES)


#
# UNIX
#

hpux:
	cc -Ae $(CPPFLAGS) -o $(PROGRAM) $(SOURCES)

hpux9:
	cc -Aa -D_HPUX_SOURCE $(CPPFLAGS) -o $(PROGRAM) $(SOURCES)


#
# Windows (32-bit)
#

win32-borlandc win32-bc:
	bcc32 -O2 -d -w -w-aus $(CPPFLAGS) $(SOURCES)

win32-cygwin32 win32-cygwin:
	gcc -mcygwin $(CPPFLAGS) $(GCC_CFLAGS) -o $(PROGRAM).exe $(SOURCES)

win32-digitalmars win32-dm:
	dmc -mn -o -w- $(CPPFLAGS) $(SOURCES)

win32-intelc win32-ic:
	icl -nologo -MT -W3 -O2 -GF $(CPPFLAGS) $(SOURCES)

win32-lccwin32:
	@echo "NOTE: need lcc 2002-07-25 or newer, older versions have bugs"
	lc -A -unused -O $(CPPFLAGS) $(SOURCES)

win32-mingw32 win32-mingw:
	gcc -mno-cygwin $(CPPFLAGS) $(GCC_CFLAGS) -o $(PROGRAM).exe $(SOURCES)

win32-visualc win32-vc:
	cl -nologo -MT -W3 -O2 -GF $(CPPFLAGS) $(SOURCES)

win32-watcomc win32-wc:
	wcl386 -bt=nt -zq -mf -5r -zc -w5 -oneatx $(CPPFLAGS) $(SOURCES)


#
# DOS (32-bit)
#

dos32-djgpp2 dos32-dj2:
	gcc $(CPPFLAGS) $(GCC_CFLAGS) -o $(PROGRAM).exe $(SOURCES)

dos32-watcomc dos32-wc:
	wcl386 -zq -mf -bt=dos -l=dos4g -5r -ox -zc $(CPPFLAGS) $(SOURCES)


#
# other targets
#

clean:
	rm -f $(PROGRAM) $(PROGRAM).exe $(PROGRAM).map $(PROGRAM).tds
	rm -f *.err *.o *.obj

.PHONY: default clean
