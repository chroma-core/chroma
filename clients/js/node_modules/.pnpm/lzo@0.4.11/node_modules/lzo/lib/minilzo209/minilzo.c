/* minilzo.c -- mini subset of the LZO real-time data compression library

   This file is part of the LZO real-time data compression library.

   Copyright (C) 1996-2015 Markus Franz Xaver Johannes Oberhumer
   All Rights Reserved.

   The LZO library is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License as
   published by the Free Software Foundation; either version 2 of
   the License, or (at your option) any later version.

   The LZO library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with the LZO library; see the file COPYING.
   If not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

   Markus F.X.J. Oberhumer
   <markus@oberhumer.com>
   http://www.oberhumer.com/opensource/lzo/
 */

/*
 * NOTE:
 *   the full LZO package can be found at
 *   http://www.oberhumer.com/opensource/lzo/
 */

#define __LZO_IN_MINILZO 1

#if defined(LZO_CFG_FREESTANDING)
#  undef MINILZO_HAVE_CONFIG_H
#  define LZO_LIBC_FREESTANDING 1
#  define LZO_OS_FREESTANDING 1
#endif

#ifdef MINILZO_HAVE_CONFIG_H
#  include <config.h>
#endif
#include <limits.h>
#include <stddef.h>
#if defined(MINILZO_CFG_USE_INTERNAL_LZODEFS)

#ifndef __LZODEFS_H_INCLUDED
#define __LZODEFS_H_INCLUDED 1

#if defined(__CYGWIN32__) && !defined(__CYGWIN__)
#  define __CYGWIN__ __CYGWIN32__
#endif
#if 1 && defined(__INTERIX) && defined(__GNUC__) && !defined(_ALL_SOURCE)
#  define _ALL_SOURCE 1
#endif
#if defined(__mips__) && defined(__R5900__)
#  if !defined(__LONG_MAX__)
#    define __LONG_MAX__ 9223372036854775807L
#  endif
#endif
#if !defined(LZO_CFG_NO_DISABLE_WUNDEF)
#if defined(__ARMCC_VERSION)
#  pragma diag_suppress 193
#elif defined(__clang__) && defined(__clang_minor__)
#  pragma clang diagnostic ignored "-Wundef"
#elif defined(__INTEL_COMPILER)
#  pragma warning(disable: 193)
#elif defined(__KEIL__) && defined(__C166__)
#  pragma warning disable = 322
#elif defined(__GNUC__) && defined(__GNUC_MINOR__) && !defined(__PATHSCALE__)
#  if ((__GNUC__-0) >= 5 || ((__GNUC__-0) == 4 && (__GNUC_MINOR__-0) >= 2))
#    pragma GCC diagnostic ignored "-Wundef"
#  endif
#elif defined(_MSC_VER) && !defined(__clang__) && !defined(__INTEL_COMPILER) && !defined(__MWERKS__)
#  if ((_MSC_VER-0) >= 1300)
#    pragma warning(disable: 4668)
#  endif
#endif
#endif
#if 0 && defined(__POCC__) && defined(_WIN32)
#  if (__POCC__ >= 400)
#    pragma warn(disable: 2216)
#  endif
#endif
#if 0 && defined(__WATCOMC__)
#  if (__WATCOMC__ >= 1050) && (__WATCOMC__ < 1060)
#    pragma warning 203 9
#  endif
#endif
#if defined(__BORLANDC__) && defined(__MSDOS__) && !defined(__FLAT__)
#  pragma option -h
#endif
#if !(LZO_CFG_NO_DISABLE_WCRTNONSTDC)
#ifndef _CRT_NONSTDC_NO_DEPRECATE
#define _CRT_NONSTDC_NO_DEPRECATE 1
#endif
#ifndef _CRT_NONSTDC_NO_WARNINGS
#define _CRT_NONSTDC_NO_WARNINGS 1
#endif
#ifndef _CRT_SECURE_NO_DEPRECATE
#define _CRT_SECURE_NO_DEPRECATE 1
#endif
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 1
#endif
#endif
#if 0
#define LZO_0xffffUL            0xfffful
#define LZO_0xffffffffUL        0xfffffffful
#else
#define LZO_0xffffUL            65535ul
#define LZO_0xffffffffUL        4294967295ul
#endif
#define LZO_0xffffL             LZO_0xffffUL
#define LZO_0xffffffffL         LZO_0xffffffffUL
#if (LZO_0xffffL == LZO_0xffffffffL)
#  error "your preprocessor is broken 1"
#endif
#if (16ul * 16384ul != 262144ul)
#  error "your preprocessor is broken 2"
#endif
#if 0
#if (32767 >= 4294967295ul)
#  error "your preprocessor is broken 3"
#endif
#if (65535u >= 4294967295ul)
#  error "your preprocessor is broken 4"
#endif
#endif
#if defined(__COUNTER__)
#  ifndef LZO_CFG_USE_COUNTER
#  define LZO_CFG_USE_COUNTER 1
#  endif
#else
#  undef LZO_CFG_USE_COUNTER
#endif
#if (UINT_MAX == LZO_0xffffL)
#if defined(__ZTC__) && defined(__I86__) && !defined(__OS2__)
#  if !defined(MSDOS)
#    define MSDOS 1
#  endif
#  if !defined(_MSDOS)
#    define _MSDOS 1
#  endif
#elif 0 && defined(__VERSION) && defined(MB_LEN_MAX)
#  if (__VERSION == 520) && (MB_LEN_MAX == 1)
#    if !defined(__AZTEC_C__)
#      define __AZTEC_C__ __VERSION
#    endif
#    if !defined(__DOS__)
#      define __DOS__ 1
#    endif
#  endif
#endif
#endif
#if defined(_MSC_VER) && defined(M_I86HM) && (UINT_MAX == LZO_0xffffL)
#  define ptrdiff_t long
#  define _PTRDIFF_T_DEFINED 1
#endif
#if (UINT_MAX == LZO_0xffffL)
#  undef __LZO_RENAME_A
#  undef __LZO_RENAME_B
#  if defined(__AZTEC_C__) && defined(__DOS__)
#    define __LZO_RENAME_A 1
#  elif defined(_MSC_VER) && defined(MSDOS)
#    if (_MSC_VER < 600)
#      define __LZO_RENAME_A 1
#    elif (_MSC_VER < 700)
#      define __LZO_RENAME_B 1
#    endif
#  elif defined(__TSC__) && defined(__OS2__)
#    define __LZO_RENAME_A 1
#  elif defined(__MSDOS__) && defined(__TURBOC__) && (__TURBOC__ < 0x0410)
#    define __LZO_RENAME_A 1
#  elif defined(__PACIFIC__) && defined(DOS)
#    if !defined(__far)
#      define __far far
#    endif
#    if !defined(__near)
#      define __near near
#    endif
#  endif
#  if defined(__LZO_RENAME_A)
#    if !defined(__cdecl)
#      define __cdecl cdecl
#    endif
#    if !defined(__far)
#      define __far far
#    endif
#    if !defined(__huge)
#      define __huge huge
#    endif
#    if !defined(__near)
#      define __near near
#    endif
#    if !defined(__pascal)
#      define __pascal pascal
#    endif
#    if !defined(__huge)
#      define __huge huge
#    endif
#  elif defined(__LZO_RENAME_B)
#    if !defined(__cdecl)
#      define __cdecl _cdecl
#    endif
#    if !defined(__far)
#      define __far _far
#    endif
#    if !defined(__huge)
#      define __huge _huge
#    endif
#    if !defined(__near)
#      define __near _near
#    endif
#    if !defined(__pascal)
#      define __pascal _pascal
#    endif
#  elif (defined(__PUREC__) || defined(__TURBOC__)) && defined(__TOS__)
#    if !defined(__cdecl)
#      define __cdecl cdecl
#    endif
#    if !defined(__pascal)
#      define __pascal pascal
#    endif
#  endif
#  undef __LZO_RENAME_A
#  undef __LZO_RENAME_B
#endif
#if (UINT_MAX == LZO_0xffffL)
#if defined(__AZTEC_C__) && defined(__DOS__)
#  define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#elif defined(_MSC_VER) && defined(MSDOS)
#  if (_MSC_VER < 600)
#    define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#  endif
#  if (_MSC_VER < 700)
#    define LZO_BROKEN_INTEGRAL_PROMOTION 1
#    define LZO_BROKEN_SIZEOF 1
#  endif
#elif defined(__PACIFIC__) && defined(DOS)
#  define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#elif defined(__TURBOC__) && defined(__MSDOS__)
#  if (__TURBOC__ < 0x0150)
#    define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#    define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#    define LZO_BROKEN_INTEGRAL_PROMOTION 1
#  endif
#  if (__TURBOC__ < 0x0200)
#    define LZO_BROKEN_SIZEOF 1
#  endif
#  if (__TURBOC__ < 0x0400) && defined(__cplusplus)
#    define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#  endif
#elif (defined(__PUREC__) || defined(__TURBOC__)) && defined(__TOS__)
#  define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#  define LZO_BROKEN_SIZEOF 1
#endif
#endif
#if defined(__WATCOMC__) && (__WATCOMC__ < 900)
#  define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#endif
#if defined(_CRAY) && defined(_CRAY1)
#  define LZO_BROKEN_SIGNED_RIGHT_SHIFT 1
#endif
#define LZO_PP_STRINGIZE(x)             #x
#define LZO_PP_MACRO_EXPAND(x)          LZO_PP_STRINGIZE(x)
#define LZO_PP_CONCAT0()                /*empty*/
#define LZO_PP_CONCAT1(a)               a
#define LZO_PP_CONCAT2(a,b)             a ## b
#define LZO_PP_CONCAT3(a,b,c)           a ## b ## c
#define LZO_PP_CONCAT4(a,b,c,d)         a ## b ## c ## d
#define LZO_PP_CONCAT5(a,b,c,d,e)       a ## b ## c ## d ## e
#define LZO_PP_CONCAT6(a,b,c,d,e,f)     a ## b ## c ## d ## e ## f
#define LZO_PP_CONCAT7(a,b,c,d,e,f,g)   a ## b ## c ## d ## e ## f ## g
#define LZO_PP_ECONCAT0()               LZO_PP_CONCAT0()
#define LZO_PP_ECONCAT1(a)              LZO_PP_CONCAT1(a)
#define LZO_PP_ECONCAT2(a,b)            LZO_PP_CONCAT2(a,b)
#define LZO_PP_ECONCAT3(a,b,c)          LZO_PP_CONCAT3(a,b,c)
#define LZO_PP_ECONCAT4(a,b,c,d)        LZO_PP_CONCAT4(a,b,c,d)
#define LZO_PP_ECONCAT5(a,b,c,d,e)      LZO_PP_CONCAT5(a,b,c,d,e)
#define LZO_PP_ECONCAT6(a,b,c,d,e,f)    LZO_PP_CONCAT6(a,b,c,d,e,f)
#define LZO_PP_ECONCAT7(a,b,c,d,e,f,g)  LZO_PP_CONCAT7(a,b,c,d,e,f,g)
#define LZO_PP_EMPTY                    /*empty*/
#define LZO_PP_EMPTY0()                 /*empty*/
#define LZO_PP_EMPTY1(a)                /*empty*/
#define LZO_PP_EMPTY2(a,b)              /*empty*/
#define LZO_PP_EMPTY3(a,b,c)            /*empty*/
#define LZO_PP_EMPTY4(a,b,c,d)          /*empty*/
#define LZO_PP_EMPTY5(a,b,c,d,e)        /*empty*/
#define LZO_PP_EMPTY6(a,b,c,d,e,f)      /*empty*/
#define LZO_PP_EMPTY7(a,b,c,d,e,f,g)    /*empty*/
#if 1
#define LZO_CPP_STRINGIZE(x)            #x
#define LZO_CPP_MACRO_EXPAND(x)         LZO_CPP_STRINGIZE(x)
#define LZO_CPP_CONCAT2(a,b)            a ## b
#define LZO_CPP_CONCAT3(a,b,c)          a ## b ## c
#define LZO_CPP_CONCAT4(a,b,c,d)        a ## b ## c ## d
#define LZO_CPP_CONCAT5(a,b,c,d,e)      a ## b ## c ## d ## e
#define LZO_CPP_CONCAT6(a,b,c,d,e,f)    a ## b ## c ## d ## e ## f
#define LZO_CPP_CONCAT7(a,b,c,d,e,f,g)  a ## b ## c ## d ## e ## f ## g
#define LZO_CPP_ECONCAT2(a,b)           LZO_CPP_CONCAT2(a,b)
#define LZO_CPP_ECONCAT3(a,b,c)         LZO_CPP_CONCAT3(a,b,c)
#define LZO_CPP_ECONCAT4(a,b,c,d)       LZO_CPP_CONCAT4(a,b,c,d)
#define LZO_CPP_ECONCAT5(a,b,c,d,e)     LZO_CPP_CONCAT5(a,b,c,d,e)
#define LZO_CPP_ECONCAT6(a,b,c,d,e,f)   LZO_CPP_CONCAT6(a,b,c,d,e,f)
#define LZO_CPP_ECONCAT7(a,b,c,d,e,f,g) LZO_CPP_CONCAT7(a,b,c,d,e,f,g)
#endif
#define __LZO_MASK_GEN(o,b)     (((((o) << ((b)-!!(b))) - (o)) << 1) + (o)*!!(b))
#if 1 && defined(__cplusplus)
#  if !defined(__STDC_CONSTANT_MACROS)
#    define __STDC_CONSTANT_MACROS 1
#  endif
#  if !defined(__STDC_LIMIT_MACROS)
#    define __STDC_LIMIT_MACROS 1
#  endif
#endif
#if defined(__cplusplus)
#  define LZO_EXTERN_C          extern "C"
#  define LZO_EXTERN_C_BEGIN    extern "C" {
#  define LZO_EXTERN_C_END      }
#else
#  define LZO_EXTERN_C          extern
#  define LZO_EXTERN_C_BEGIN    /*empty*/
#  define LZO_EXTERN_C_END      /*empty*/
#endif
#if !defined(__LZO_OS_OVERRIDE)
#if (LZO_OS_FREESTANDING)
#  define LZO_INFO_OS           "freestanding"
#elif (LZO_OS_EMBEDDED)
#  define LZO_INFO_OS           "embedded"
#elif 1 && defined(__IAR_SYSTEMS_ICC__)
#  define LZO_OS_EMBEDDED       1
#  define LZO_INFO_OS           "embedded"
#elif defined(__CYGWIN__) && defined(__GNUC__)
#  define LZO_OS_CYGWIN         1
#  define LZO_INFO_OS           "cygwin"
#elif defined(__EMX__) && defined(__GNUC__)
#  define LZO_OS_EMX            1
#  define LZO_INFO_OS           "emx"
#elif defined(__BEOS__)
#  define LZO_OS_BEOS           1
#  define LZO_INFO_OS           "beos"
#elif defined(__Lynx__)
#  define LZO_OS_LYNXOS         1
#  define LZO_INFO_OS           "lynxos"
#elif defined(__OS400__)
#  define LZO_OS_OS400          1
#  define LZO_INFO_OS           "os400"
#elif defined(__QNX__)
#  define LZO_OS_QNX            1
#  define LZO_INFO_OS           "qnx"
#elif defined(__BORLANDC__) && defined(__DPMI32__) && (__BORLANDC__ >= 0x0460)
#  define LZO_OS_DOS32          1
#  define LZO_INFO_OS           "dos32"
#elif defined(__BORLANDC__) && defined(__DPMI16__)
#  define LZO_OS_DOS16          1
#  define LZO_INFO_OS           "dos16"
#elif defined(__ZTC__) && defined(DOS386)
#  define LZO_OS_DOS32          1
#  define LZO_INFO_OS           "dos32"
#elif defined(__OS2__) || defined(__OS2V2__)
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_OS216        1
#    define LZO_INFO_OS         "os216"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_OS2          1
#    define LZO_INFO_OS         "os2"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__WIN64__) || defined(_WIN64) || defined(WIN64)
#  define LZO_OS_WIN64          1
#  define LZO_INFO_OS           "win64"
#elif defined(__WIN32__) || defined(_WIN32) || defined(WIN32) || defined(__WINDOWS_386__)
#  define LZO_OS_WIN32          1
#  define LZO_INFO_OS           "win32"
#elif defined(__MWERKS__) && defined(__INTEL__)
#  define LZO_OS_WIN32          1
#  define LZO_INFO_OS           "win32"
#elif defined(__WINDOWS__) || defined(_WINDOWS) || defined(_Windows)
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_WIN16        1
#    define LZO_INFO_OS         "win16"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_WIN32        1
#    define LZO_INFO_OS         "win32"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__DOS__) || defined(__MSDOS__) || defined(_MSDOS) || defined(MSDOS) || (defined(__PACIFIC__) && defined(DOS))
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_DOS16        1
#    define LZO_INFO_OS         "dos16"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_DOS32        1
#    define LZO_INFO_OS         "dos32"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__WATCOMC__)
#  if defined(__NT__) && (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_DOS16        1
#    define LZO_INFO_OS         "dos16"
#  elif defined(__NT__) && (__WATCOMC__ < 1100)
#    define LZO_OS_WIN32        1
#    define LZO_INFO_OS         "win32"
#  elif defined(__linux__) || defined(__LINUX__)
#    define LZO_OS_POSIX        1
#    define LZO_INFO_OS         "posix"
#  else
#    error "please specify a target using the -bt compiler option"
#  endif
#elif defined(__palmos__)
#  define LZO_OS_PALMOS         1
#  define LZO_INFO_OS           "palmos"
#elif defined(__TOS__) || defined(__atarist__)
#  define LZO_OS_TOS            1
#  define LZO_INFO_OS           "tos"
#elif defined(macintosh) && !defined(__arm__) && !defined(__i386__) && !defined(__ppc__) && !defined(__x64_64__)
#  define LZO_OS_MACCLASSIC     1
#  define LZO_INFO_OS           "macclassic"
#elif defined(__VMS)
#  define LZO_OS_VMS            1
#  define LZO_INFO_OS           "vms"
#elif (defined(__mips__) && defined(__R5900__)) || defined(__MIPS_PSX2__)
#  define LZO_OS_CONSOLE        1
#  define LZO_OS_CONSOLE_PS2    1
#  define LZO_INFO_OS           "console"
#  define LZO_INFO_OS_CONSOLE   "ps2"
#elif defined(__mips__) && defined(__psp__)
#  define LZO_OS_CONSOLE        1
#  define LZO_OS_CONSOLE_PSP    1
#  define LZO_INFO_OS           "console"
#  define LZO_INFO_OS_CONSOLE   "psp"
#else
#  define LZO_OS_POSIX          1
#  define LZO_INFO_OS           "posix"
#endif
#if (LZO_OS_POSIX)
#  if defined(_AIX) || defined(__AIX__) || defined(__aix__)
#    define LZO_OS_POSIX_AIX        1
#    define LZO_INFO_OS_POSIX       "aix"
#  elif defined(__FreeBSD__)
#    define LZO_OS_POSIX_FREEBSD    1
#    define LZO_INFO_OS_POSIX       "freebsd"
#  elif defined(__hpux__) || defined(__hpux)
#    define LZO_OS_POSIX_HPUX       1
#    define LZO_INFO_OS_POSIX       "hpux"
#  elif defined(__INTERIX)
#    define LZO_OS_POSIX_INTERIX    1
#    define LZO_INFO_OS_POSIX       "interix"
#  elif defined(__IRIX__) || defined(__irix__)
#    define LZO_OS_POSIX_IRIX       1
#    define LZO_INFO_OS_POSIX       "irix"
#  elif defined(__linux__) || defined(__linux) || defined(__LINUX__)
#    define LZO_OS_POSIX_LINUX      1
#    define LZO_INFO_OS_POSIX       "linux"
#  elif defined(__APPLE__) && defined(__MACH__)
#    if ((__ENVIRONMENT_IPHONE_OS_VERSION_MIN_REQUIRED__-0) >= 20000)
#      define LZO_OS_POSIX_DARWIN     1040
#      define LZO_INFO_OS_POSIX       "darwin_iphone"
#    elif ((__ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__-0) >= 1040)
#      define LZO_OS_POSIX_DARWIN     __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__
#      define LZO_INFO_OS_POSIX       "darwin"
#    else
#      define LZO_OS_POSIX_DARWIN     1
#      define LZO_INFO_OS_POSIX       "darwin"
#    endif
#    define LZO_OS_POSIX_MACOSX     LZO_OS_POSIX_DARWIN
#  elif defined(__minix__) || defined(__minix)
#    define LZO_OS_POSIX_MINIX      1
#    define LZO_INFO_OS_POSIX       "minix"
#  elif defined(__NetBSD__)
#    define LZO_OS_POSIX_NETBSD     1
#    define LZO_INFO_OS_POSIX       "netbsd"
#  elif defined(__OpenBSD__)
#    define LZO_OS_POSIX_OPENBSD    1
#    define LZO_INFO_OS_POSIX       "openbsd"
#  elif defined(__osf__)
#    define LZO_OS_POSIX_OSF        1
#    define LZO_INFO_OS_POSIX       "osf"
#  elif defined(__solaris__) || defined(__sun)
#    if defined(__SVR4) || defined(__svr4__)
#      define LZO_OS_POSIX_SOLARIS  1
#      define LZO_INFO_OS_POSIX     "solaris"
#    else
#      define LZO_OS_POSIX_SUNOS    1
#      define LZO_INFO_OS_POSIX     "sunos"
#    endif
#  elif defined(__ultrix__) || defined(__ultrix)
#    define LZO_OS_POSIX_ULTRIX     1
#    define LZO_INFO_OS_POSIX       "ultrix"
#  elif defined(_UNICOS)
#    define LZO_OS_POSIX_UNICOS     1
#    define LZO_INFO_OS_POSIX       "unicos"
#  else
#    define LZO_OS_POSIX_UNKNOWN    1
#    define LZO_INFO_OS_POSIX       "unknown"
#  endif
#endif
#endif
#if (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#  if (UINT_MAX != LZO_0xffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#endif
#if (LZO_OS_DOS32 || LZO_OS_OS2 || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (UINT_MAX != LZO_0xffffffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#endif
#if defined(CIL) && defined(_GNUCC) && defined(__GNUC__)
#  define LZO_CC_CILLY          1
#  define LZO_INFO_CC           "Cilly"
#  if defined(__CILLY__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__CILLY__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif 0 && defined(SDCC) && defined(__VERSION__) && !defined(__GNUC__)
#  define LZO_CC_SDCC           1
#  define LZO_INFO_CC           "sdcc"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(SDCC)
#elif defined(__PATHSCALE__) && defined(__PATHCC_PATCHLEVEL__)
#  define LZO_CC_PATHSCALE      (__PATHCC__ * 0x10000L + (__PATHCC_MINOR__-0) * 0x100 + (__PATHCC_PATCHLEVEL__-0))
#  define LZO_INFO_CC           "Pathscale C"
#  define LZO_INFO_CCVER        __PATHSCALE__
#  if defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#    define LZO_CC_PATHSCALE_GNUC (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  endif
#elif defined(__INTEL_COMPILER) && ((__INTEL_COMPILER-0) > 0)
#  define LZO_CC_INTELC         __INTEL_COMPILER
#  define LZO_INFO_CC           "Intel C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__INTEL_COMPILER)
#  if defined(_MSC_VER) && ((_MSC_VER-0) > 0)
#    define LZO_CC_INTELC_MSC   _MSC_VER
#  elif defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#    define LZO_CC_INTELC_GNUC   (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  endif
#elif defined(__POCC__) && defined(_WIN32)
#  define LZO_CC_PELLESC        1
#  define LZO_INFO_CC           "Pelles C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__POCC__)
#elif defined(__ARMCC_VERSION) && defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#  if defined(__GNUC_PATCHLEVEL__)
#    define LZO_CC_ARMCC_GNUC   (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  else
#    define LZO_CC_ARMCC_GNUC   (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100)
#  endif
#  define LZO_CC_ARMCC          __ARMCC_VERSION
#  define LZO_INFO_CC           "ARM C Compiler"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(__clang__) && defined(__llvm__) && defined(__VERSION__)
#  if defined(__clang_major__) && defined(__clang_minor__) && defined(__clang_patchlevel__)
#    define LZO_CC_CLANG        (__clang_major__ * 0x10000L + (__clang_minor__-0) * 0x100 + (__clang_patchlevel__-0))
#  else
#    define LZO_CC_CLANG        0x010000L
#  endif
#  if defined(_MSC_VER) && ((_MSC_VER-0) > 0)
#    define LZO_CC_CLANG_MSC    _MSC_VER
#  elif defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#    define LZO_CC_CLANG_GNUC   (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  endif
#  define LZO_INFO_CC           "clang"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(__llvm__) && defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#  if defined(__GNUC_PATCHLEVEL__)
#    define LZO_CC_LLVM_GNUC    (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  else
#    define LZO_CC_LLVM_GNUC    (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100)
#  endif
#  define LZO_CC_LLVM           LZO_CC_LLVM_GNUC
#  define LZO_INFO_CC           "llvm-gcc"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(__ACK__) && defined(_ACK)
#  define LZO_CC_ACK            1
#  define LZO_INFO_CC           "Amsterdam Compiler Kit C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__ARMCC_VERSION) && !defined(__GNUC__)
#  define LZO_CC_ARMCC          __ARMCC_VERSION
#  define LZO_CC_ARMCC_ARMCC    __ARMCC_VERSION
#  define LZO_INFO_CC           "ARM C Compiler"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__ARMCC_VERSION)
#elif defined(__AZTEC_C__)
#  define LZO_CC_AZTECC         1
#  define LZO_INFO_CC           "Aztec C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__AZTEC_C__)
#elif defined(__CODEGEARC__)
#  define LZO_CC_CODEGEARC      1
#  define LZO_INFO_CC           "CodeGear C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__CODEGEARC__)
#elif defined(__BORLANDC__)
#  define LZO_CC_BORLANDC       1
#  define LZO_INFO_CC           "Borland C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__BORLANDC__)
#elif defined(_CRAYC) && defined(_RELEASE)
#  define LZO_CC_CRAYC          1
#  define LZO_INFO_CC           "Cray C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(_RELEASE)
#elif defined(__DMC__) && defined(__SC__)
#  define LZO_CC_DMC            1
#  define LZO_INFO_CC           "Digital Mars C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__DMC__)
#elif defined(__DECC)
#  define LZO_CC_DECC           1
#  define LZO_INFO_CC           "DEC C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__DECC)
#elif (defined(__ghs) || defined(__ghs__)) && defined(__GHS_VERSION_NUMBER) && ((__GHS_VERSION_NUMBER-0) > 0)
#  define LZO_CC_GHS            1
#  define LZO_INFO_CC           "Green Hills C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__GHS_VERSION_NUMBER)
#  if defined(_MSC_VER) && ((_MSC_VER-0) > 0)
#    define LZO_CC_GHS_MSC      _MSC_VER
#  elif defined(__GNUC__) && defined(__GNUC_MINOR__) && defined(__VERSION__)
#    define LZO_CC_GHS_GNUC     (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  endif
#elif defined(__HIGHC__)
#  define LZO_CC_HIGHC          1
#  define LZO_INFO_CC           "MetaWare High C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__HP_aCC) && ((__HP_aCC-0) > 0)
#  define LZO_CC_HPACC          __HP_aCC
#  define LZO_INFO_CC           "HP aCC"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__HP_aCC)
#elif defined(__IAR_SYSTEMS_ICC__)
#  define LZO_CC_IARC           1
#  define LZO_INFO_CC           "IAR C"
#  if defined(__VER__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__VER__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__IBMC__) && ((__IBMC__-0) > 0)
#  define LZO_CC_IBMC           __IBMC__
#  define LZO_INFO_CC           "IBM C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__IBMC__)
#elif defined(__IBMCPP__) && ((__IBMCPP__-0) > 0)
#  define LZO_CC_IBMC           __IBMCPP__
#  define LZO_INFO_CC           "IBM C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__IBMCPP__)
#elif defined(__KEIL__) && defined(__C166__)
#  define LZO_CC_KEILC          1
#  define LZO_INFO_CC           "Keil C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__C166__)
#elif defined(__LCC__) && defined(_WIN32) && defined(__LCCOPTIMLEVEL)
#  define LZO_CC_LCCWIN32       1
#  define LZO_INFO_CC           "lcc-win32"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__LCC__)
#  define LZO_CC_LCC            1
#  define LZO_INFO_CC           "lcc"
#  if defined(__LCC_VERSION__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__LCC_VERSION__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__MWERKS__) && ((__MWERKS__-0) > 0)
#  define LZO_CC_MWERKS         __MWERKS__
#  define LZO_INFO_CC           "Metrowerks C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__MWERKS__)
#elif (defined(__NDPC__) || defined(__NDPX__)) && defined(__i386)
#  define LZO_CC_NDPC           1
#  define LZO_INFO_CC           "Microway NDP C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__PACIFIC__)
#  define LZO_CC_PACIFICC       1
#  define LZO_INFO_CC           "Pacific C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__PACIFIC__)
#elif defined(__PGI) && defined(__PGIC__) && defined(__PGIC_MINOR__)
#  if defined(__PGIC_PATCHLEVEL__)
#    define LZO_CC_PGI          (__PGIC__ * 0x10000L + (__PGIC_MINOR__-0) * 0x100 + (__PGIC_PATCHLEVEL__-0))
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__PGIC__) "." LZO_PP_MACRO_EXPAND(__PGIC_MINOR__) "." LZO_PP_MACRO_EXPAND(__PGIC_PATCHLEVEL__)
#  else
#    define LZO_CC_PGI          (__PGIC__ * 0x10000L + (__PGIC_MINOR__-0) * 0x100)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__PGIC__) "." LZO_PP_MACRO_EXPAND(__PGIC_MINOR__) ".0"
#  endif
#  define LZO_INFO_CC           "Portland Group PGI C"
#elif defined(__PGI) && (defined(__linux__) || defined(__WIN32__))
#  define LZO_CC_PGI            1
#  define LZO_INFO_CC           "Portland Group PGI C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__PUREC__) && defined(__TOS__)
#  define LZO_CC_PUREC          1
#  define LZO_INFO_CC           "Pure C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__PUREC__)
#elif defined(__SC__) && defined(__ZTC__)
#  define LZO_CC_SYMANTECC      1
#  define LZO_INFO_CC           "Symantec C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__SC__)
#elif defined(__SUNPRO_C)
#  define LZO_INFO_CC           "SunPro C"
#  if ((__SUNPRO_C-0) > 0)
#    define LZO_CC_SUNPROC      __SUNPRO_C
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__SUNPRO_C)
#  else
#    define LZO_CC_SUNPROC      1
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__SUNPRO_CC)
#  define LZO_INFO_CC           "SunPro C"
#  if ((__SUNPRO_CC-0) > 0)
#    define LZO_CC_SUNPROC      __SUNPRO_CC
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__SUNPRO_CC)
#  else
#    define LZO_CC_SUNPROC      1
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__TINYC__)
#  define LZO_CC_TINYC          1
#  define LZO_INFO_CC           "Tiny C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TINYC__)
#elif defined(__TSC__)
#  define LZO_CC_TOPSPEEDC      1
#  define LZO_INFO_CC           "TopSpeed C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TSC__)
#elif defined(__WATCOMC__)
#  define LZO_CC_WATCOMC        1
#  define LZO_INFO_CC           "Watcom C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__WATCOMC__)
#elif defined(__TURBOC__)
#  define LZO_CC_TURBOC         1
#  define LZO_INFO_CC           "Turbo C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TURBOC__)
#elif defined(__ZTC__)
#  define LZO_CC_ZORTECHC       1
#  define LZO_INFO_CC           "Zortech C"
#  if ((__ZTC__-0) == 0x310)
#    define LZO_INFO_CCVER      "0x310"
#  else
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__ZTC__)
#  endif
#elif defined(__GNUC__) && defined(__VERSION__)
#  if defined(__GNUC_MINOR__) && defined(__GNUC_PATCHLEVEL__)
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100 + (__GNUC_PATCHLEVEL__-0))
#  elif defined(__GNUC_MINOR__)
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L + (__GNUC_MINOR__-0) * 0x100)
#  else
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L)
#  endif
#  define LZO_INFO_CC           "gcc"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(_MSC_VER) && ((_MSC_VER-0) > 0)
#  define LZO_CC_MSC            _MSC_VER
#  define LZO_INFO_CC           "Microsoft C"
#  if defined(_MSC_FULL_VER)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(_MSC_VER) "." LZO_PP_MACRO_EXPAND(_MSC_FULL_VER)
#  else
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(_MSC_VER)
#  endif
#else
#  define LZO_CC_UNKNOWN        1
#  define LZO_INFO_CC           "unknown"
#  define LZO_INFO_CCVER        "unknown"
#endif
#if (LZO_CC_GNUC) && defined(__OPEN64__)
#  if defined(__OPENCC__) && defined(__OPENCC_MINOR__) && defined(__OPENCC_PATCHLEVEL__)
#    define LZO_CC_OPEN64       (__OPENCC__ * 0x10000L + (__OPENCC_MINOR__-0) * 0x100 + (__OPENCC_PATCHLEVEL__-0))
#    define LZO_CC_OPEN64_GNUC  LZO_CC_GNUC
#  endif
#endif
#if (LZO_CC_GNUC) && defined(__PCC__)
#  if defined(__PCC__) && defined(__PCC_MINOR__) && defined(__PCC_MINORMINOR__)
#    define LZO_CC_PCC          (__PCC__ * 0x10000L + (__PCC_MINOR__-0) * 0x100 + (__PCC_MINORMINOR__-0))
#    define LZO_CC_PCC_GNUC     LZO_CC_GNUC
#  endif
#endif
#if 0 && (LZO_CC_MSC && (_MSC_VER >= 1200)) && !defined(_MSC_FULL_VER)
#  error "LZO_CC_MSC: _MSC_FULL_VER is not defined"
#endif
#if !defined(__LZO_ARCH_OVERRIDE) && !(LZO_ARCH_GENERIC) && defined(_CRAY)
#  if (UINT_MAX > LZO_0xffffffffL) && defined(_CRAY)
#    if defined(_CRAYMPP) || defined(_CRAYT3D) || defined(_CRAYT3E)
#      define LZO_ARCH_CRAY_MPP     1
#    elif defined(_CRAY1)
#      define LZO_ARCH_CRAY_PVP     1
#    endif
#  endif
#endif
#if !defined(__LZO_ARCH_OVERRIDE)
#if (LZO_ARCH_GENERIC)
#  define LZO_INFO_ARCH             "generic"
#elif (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#  define LZO_ARCH_I086             1
#  define LZO_INFO_ARCH             "i086"
#elif defined(__aarch64__)
#  define LZO_ARCH_ARM64            1
#  define LZO_INFO_ARCH             "arm64"
#elif defined(__alpha__) || defined(__alpha) || defined(_M_ALPHA)
#  define LZO_ARCH_ALPHA            1
#  define LZO_INFO_ARCH             "alpha"
#elif (LZO_ARCH_CRAY_MPP) && (defined(_CRAYT3D) || defined(_CRAYT3E))
#  define LZO_ARCH_ALPHA            1
#  define LZO_INFO_ARCH             "alpha"
#elif defined(__amd64__) || defined(__x86_64__) || defined(_M_AMD64)
#  define LZO_ARCH_AMD64            1
#  define LZO_INFO_ARCH             "amd64"
#elif defined(__arm__) || defined(_M_ARM)
#  define LZO_ARCH_ARM              1
#  define LZO_INFO_ARCH             "arm"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCARM__)
#  define LZO_ARCH_ARM              1
#  define LZO_INFO_ARCH             "arm"
#elif (UINT_MAX <= LZO_0xffffL) && defined(__AVR__)
#  define LZO_ARCH_AVR              1
#  define LZO_INFO_ARCH             "avr"
#elif defined(__avr32__) || defined(__AVR32__)
#  define LZO_ARCH_AVR32            1
#  define LZO_INFO_ARCH             "avr32"
#elif defined(__bfin__)
#  define LZO_ARCH_BLACKFIN         1
#  define LZO_INFO_ARCH             "blackfin"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C166__)
#  define LZO_ARCH_C166             1
#  define LZO_INFO_ARCH             "c166"
#elif defined(__cris__)
#  define LZO_ARCH_CRIS             1
#  define LZO_INFO_ARCH             "cris"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCEZ80__)
#  define LZO_ARCH_EZ80             1
#  define LZO_INFO_ARCH             "ez80"
#elif defined(__H8300__) || defined(__H8300H__) || defined(__H8300S__) || defined(__H8300SX__)
#  define LZO_ARCH_H8300            1
#  define LZO_INFO_ARCH             "h8300"
#elif defined(__hppa__) || defined(__hppa)
#  define LZO_ARCH_HPPA             1
#  define LZO_INFO_ARCH             "hppa"
#elif defined(__386__) || defined(__i386__) || defined(__i386) || defined(_M_IX86) || defined(_M_I386)
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif (LZO_CC_ZORTECHC && defined(__I86__))
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif (LZO_OS_DOS32 && LZO_CC_HIGHC) && defined(_I386)
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#  define LZO_ARCH_IA64             1
#  define LZO_INFO_ARCH             "ia64"
#elif (UINT_MAX == LZO_0xffffL) && defined(__m32c__)
#  define LZO_ARCH_M16C             1
#  define LZO_INFO_ARCH             "m16c"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCM16C__)
#  define LZO_ARCH_M16C             1
#  define LZO_INFO_ARCH             "m16c"
#elif defined(__m32r__)
#  define LZO_ARCH_M32R             1
#  define LZO_INFO_ARCH             "m32r"
#elif (LZO_OS_TOS) || defined(__m68k__) || defined(__m68000__) || defined(__mc68000__) || defined(__mc68020__) || defined(_M_M68K)
#  define LZO_ARCH_M68K             1
#  define LZO_INFO_ARCH             "m68k"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C251__)
#  define LZO_ARCH_MCS251           1
#  define LZO_INFO_ARCH             "mcs251"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C51__)
#  define LZO_ARCH_MCS51            1
#  define LZO_INFO_ARCH             "mcs51"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICC8051__)
#  define LZO_ARCH_MCS51            1
#  define LZO_INFO_ARCH             "mcs51"
#elif defined(__mips__) || defined(__mips) || defined(_MIPS_ARCH) || defined(_M_MRX000)
#  define LZO_ARCH_MIPS             1
#  define LZO_INFO_ARCH             "mips"
#elif (UINT_MAX == LZO_0xffffL) && defined(__MSP430__)
#  define LZO_ARCH_MSP430           1
#  define LZO_INFO_ARCH             "msp430"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICC430__)
#  define LZO_ARCH_MSP430           1
#  define LZO_INFO_ARCH             "msp430"
#elif defined(__powerpc__) || defined(__powerpc) || defined(__ppc__) || defined(__PPC__) || defined(_M_PPC) || defined(_ARCH_PPC) || defined(_ARCH_PWR)
#  define LZO_ARCH_POWERPC          1
#  define LZO_INFO_ARCH             "powerpc"
#elif defined(__s390__) || defined(__s390) || defined(__s390x__) || defined(__s390x)
#  define LZO_ARCH_S390             1
#  define LZO_INFO_ARCH             "s390"
#elif defined(__sh__) || defined(_M_SH)
#  define LZO_ARCH_SH               1
#  define LZO_INFO_ARCH             "sh"
#elif defined(__sparc__) || defined(__sparc) || defined(__sparcv8)
#  define LZO_ARCH_SPARC            1
#  define LZO_INFO_ARCH             "sparc"
#elif defined(__SPU__)
#  define LZO_ARCH_SPU              1
#  define LZO_INFO_ARCH             "spu"
#elif (UINT_MAX == LZO_0xffffL) && defined(__z80)
#  define LZO_ARCH_Z80              1
#  define LZO_INFO_ARCH             "z80"
#elif (LZO_ARCH_CRAY_PVP)
#  if defined(_CRAYSV1)
#    define LZO_ARCH_CRAY_SV1       1
#    define LZO_INFO_ARCH           "cray_sv1"
#  elif (_ADDR64)
#    define LZO_ARCH_CRAY_T90       1
#    define LZO_INFO_ARCH           "cray_t90"
#  elif (_ADDR32)
#    define LZO_ARCH_CRAY_YMP       1
#    define LZO_INFO_ARCH           "cray_ymp"
#  else
#    define LZO_ARCH_CRAY_XMP       1
#    define LZO_INFO_ARCH           "cray_xmp"
#  endif
#else
#  define LZO_ARCH_UNKNOWN          1
#  define LZO_INFO_ARCH             "unknown"
#endif
#endif
#if !defined(LZO_ARCH_ARM_THUMB2)
#if (LZO_ARCH_ARM)
#  if defined(__ARM_ARCH_ISA_THUMB)
#   if ((__ARM_ARCH_ISA_THUMB)+0 >= 2)
#    define LZO_ARCH_ARM_THUMB2     1
#   endif
#  elif 1 && defined(__thumb2__)
#    define LZO_ARCH_ARM_THUMB2     1
#  elif 1 && defined(__TARGET_ARCH_THUMB) && ((__TARGET_ARCH_THUMB)+0 >= 4)
#    define LZO_ARCH_ARM_THUMB2     1
#  endif
#endif
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_DOS32 || LZO_OS_OS2)
#  error "FIXME - missing define for CPU architecture"
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_WIN32)
#  error "FIXME - missing LZO_OS_WIN32 define for CPU architecture"
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_WIN64)
#  error "FIXME - missing LZO_OS_WIN64 define for CPU architecture"
#endif
#if (LZO_OS_OS216 || LZO_OS_WIN16)
#  define LZO_ARCH_I086PM           1
#elif 1 && (LZO_OS_DOS16 && defined(BLX286))
#  define LZO_ARCH_I086PM           1
#elif 1 && (LZO_OS_DOS16 && defined(DOSX286))
#  define LZO_ARCH_I086PM           1
#elif 1 && (LZO_OS_DOS16 && LZO_CC_BORLANDC && defined(__DPMI16__))
#  define LZO_ARCH_I086PM           1
#endif
#if (LZO_ARCH_AMD64 && !LZO_ARCH_X64)
#  define LZO_ARCH_X64              1
#elif (!LZO_ARCH_AMD64 && LZO_ARCH_X64) && defined(__LZO_ARCH_OVERRIDE)
#  define LZO_ARCH_AMD64            1
#endif
#if (LZO_ARCH_ARM64 && !LZO_ARCH_AARCH64)
#  define LZO_ARCH_AARCH64          1
#elif (!LZO_ARCH_ARM64 && LZO_ARCH_AARCH64) && defined(__LZO_ARCH_OVERRIDE)
#  define LZO_ARCH_ARM64            1
#endif
#if (LZO_ARCH_I386 && !LZO_ARCH_X86)
#  define LZO_ARCH_X86              1
#elif (!LZO_ARCH_I386 && LZO_ARCH_X86) && defined(__LZO_ARCH_OVERRIDE)
#  define LZO_ARCH_I386            1
#endif
#if (LZO_ARCH_AMD64 && !LZO_ARCH_X64) || (!LZO_ARCH_AMD64 && LZO_ARCH_X64)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_ARM64 && !LZO_ARCH_AARCH64) || (!LZO_ARCH_ARM64 && LZO_ARCH_AARCH64)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_I386 && !LZO_ARCH_X86) || (!LZO_ARCH_I386 && LZO_ARCH_X86)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_ARM_THUMB1 && !LZO_ARCH_ARM)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_ARM_THUMB2 && !LZO_ARCH_ARM)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_ARM_THUMB1 && LZO_ARCH_ARM_THUMB2)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_I086PM && !LZO_ARCH_I086)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_I086)
#  if (UINT_MAX != LZO_0xffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#endif
#if (LZO_ARCH_I386)
#  if (UINT_MAX != LZO_0xffffL) && defined(__i386_int16__)
#    error "unexpected configuration - check your compiler defines"
#  endif
#  if (UINT_MAX != LZO_0xffffffffL) && !defined(__i386_int16__)
#    error "unexpected configuration - check your compiler defines"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "unexpected configuration - check your compiler defines"
#  endif
#endif
#if (LZO_ARCH_AMD64 || LZO_ARCH_I386)
#  if !defined(LZO_TARGET_FEATURE_SSE2)
#    if defined(__SSE2__)
#      define LZO_TARGET_FEATURE_SSE2       1
#    elif defined(_MSC_VER) && (defined(_M_IX86_FP) && ((_M_IX86_FP)+0 >= 2))
#      define LZO_TARGET_FEATURE_SSE2       1
#    elif (LZO_CC_INTELC_MSC || LZO_CC_MSC) && defined(_M_AMD64)
#      define LZO_TARGET_FEATURE_SSE2       1
#    endif
#  endif
#  if !defined(LZO_TARGET_FEATURE_SSSE3)
#  if (LZO_TARGET_FEATURE_SSE2)
#    if defined(__SSSE3__)
#      define LZO_TARGET_FEATURE_SSSE3      1
#    elif defined(_MSC_VER) && defined(__AVX__)
#      define LZO_TARGET_FEATURE_SSSE3      1
#    endif
#  endif
#  endif
#  if !defined(LZO_TARGET_FEATURE_SSE4_2)
#  if (LZO_TARGET_FEATURE_SSSE3)
#    if defined(__SSE4_2__)
#      define LZO_TARGET_FEATURE_SSE4_2     1
#    endif
#  endif
#  endif
#  if !defined(LZO_TARGET_FEATURE_AVX)
#  if (LZO_TARGET_FEATURE_SSSE3)
#    if defined(__AVX__)
#      define LZO_TARGET_FEATURE_AVX        1
#    endif
#  endif
#  endif
#  if !defined(LZO_TARGET_FEATURE_AVX2)
#  if (LZO_TARGET_FEATURE_AVX)
#    if defined(__AVX2__)
#      define LZO_TARGET_FEATURE_AVX2       1
#    endif
#  endif
#  endif
#endif
#if (LZO_TARGET_FEATURE_SSSE3 && !(LZO_TARGET_FEATURE_SSE2))
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_TARGET_FEATURE_SSE4_2 && !(LZO_TARGET_FEATURE_SSSE3))
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_TARGET_FEATURE_AVX && !(LZO_TARGET_FEATURE_SSSE3))
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_TARGET_FEATURE_AVX2 && !(LZO_TARGET_FEATURE_AVX))
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ARCH_ARM)
#  if !defined(LZO_TARGET_FEATURE_NEON)
#    if defined(__ARM_NEON) && ((__ARM_NEON)+0)
#      define LZO_TARGET_FEATURE_NEON       1
#    elif 1 && defined(__ARM_NEON__) && ((__ARM_NEON__)+0)
#      define LZO_TARGET_FEATURE_NEON       1
#    elif 1 && defined(__TARGET_FEATURE_NEON) && ((__TARGET_FEATURE_NEON)+0)
#      define LZO_TARGET_FEATURE_NEON       1
#    endif
#  endif
#elif (LZO_ARCH_ARM64)
#  if !defined(LZO_TARGET_FEATURE_NEON)
#    if 1
#      define LZO_TARGET_FEATURE_NEON       1
#    endif
#  endif
#endif
#if 0
#elif !defined(__LZO_MM_OVERRIDE)
#if (LZO_ARCH_I086)
#if (UINT_MAX != LZO_0xffffL)
#  error "unexpected configuration - check your compiler defines"
#endif
#if defined(__TINY__) || defined(M_I86TM) || defined(_M_I86TM)
#  define LZO_MM_TINY           1
#elif defined(__HUGE__) || defined(_HUGE_) || defined(M_I86HM) || defined(_M_I86HM)
#  define LZO_MM_HUGE           1
#elif defined(__SMALL__) || defined(M_I86SM) || defined(_M_I86SM) || defined(SMALL_MODEL)
#  define LZO_MM_SMALL          1
#elif defined(__MEDIUM__) || defined(M_I86MM) || defined(_M_I86MM)
#  define LZO_MM_MEDIUM         1
#elif defined(__COMPACT__) || defined(M_I86CM) || defined(_M_I86CM)
#  define LZO_MM_COMPACT        1
#elif defined(__LARGE__) || defined(M_I86LM) || defined(_M_I86LM) || defined(LARGE_MODEL)
#  define LZO_MM_LARGE          1
#elif (LZO_CC_AZTECC)
#  if defined(_LARGE_CODE) && defined(_LARGE_DATA)
#    define LZO_MM_LARGE        1
#  elif defined(_LARGE_CODE)
#    define LZO_MM_MEDIUM       1
#  elif defined(_LARGE_DATA)
#    define LZO_MM_COMPACT      1
#  else
#    define LZO_MM_SMALL        1
#  endif
#elif (LZO_CC_ZORTECHC && defined(__VCM__))
#  define LZO_MM_LARGE          1
#else
#  error "unknown LZO_ARCH_I086 memory model"
#endif
#if (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#define LZO_HAVE_MM_HUGE_PTR        1
#define LZO_HAVE_MM_HUGE_ARRAY      1
#if (LZO_MM_TINY)
#  undef LZO_HAVE_MM_HUGE_ARRAY
#endif
#if (LZO_CC_AZTECC || LZO_CC_PACIFICC || LZO_CC_ZORTECHC)
#  undef LZO_HAVE_MM_HUGE_PTR
#  undef LZO_HAVE_MM_HUGE_ARRAY
#elif (LZO_CC_DMC || LZO_CC_SYMANTECC)
#  undef LZO_HAVE_MM_HUGE_ARRAY
#elif (LZO_CC_MSC && defined(_QC))
#  undef LZO_HAVE_MM_HUGE_ARRAY
#  if (_MSC_VER < 600)
#    undef LZO_HAVE_MM_HUGE_PTR
#  endif
#elif (LZO_CC_TURBOC && (__TURBOC__ < 0x0295))
#  undef LZO_HAVE_MM_HUGE_ARRAY
#endif
#if (LZO_ARCH_I086PM) && !(LZO_HAVE_MM_HUGE_PTR)
#  if (LZO_OS_DOS16)
#    error "unexpected configuration - check your compiler defines"
#  elif (LZO_CC_ZORTECHC)
#  else
#    error "unexpected configuration - check your compiler defines"
#  endif
#endif
#ifdef __cplusplus
extern "C" {
#endif
#if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0200))
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_DMC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_MSC || LZO_CC_TOPSPEEDC)
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_TURBOC && (__TURBOC__ >= 0x0295))
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif ((LZO_CC_AZTECC || LZO_CC_PACIFICC || LZO_CC_TURBOC) && LZO_OS_DOS16)
#  define LZO_MM_AHSHIFT      12
#elif (LZO_CC_WATCOMC)
   extern unsigned char _HShift;
#  define LZO_MM_AHSHIFT      ((unsigned) _HShift)
#else
#  error "FIXME - implement LZO_MM_AHSHIFT"
#endif
#ifdef __cplusplus
}
#endif
#endif
#elif (LZO_ARCH_C166)
#if !defined(__MODEL__)
#  error "FIXME - LZO_ARCH_C166 __MODEL__"
#elif ((__MODEL__) == 0)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 1)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - LZO_ARCH_C166 __MODEL__"
#endif
#elif (LZO_ARCH_MCS251)
#if !defined(__MODEL__)
#  error "FIXME - LZO_ARCH_MCS251 __MODEL__"
#elif ((__MODEL__) == 0)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - LZO_ARCH_MCS251 __MODEL__"
#endif
#elif (LZO_ARCH_MCS51)
#if !defined(__MODEL__)
#  error "FIXME - LZO_ARCH_MCS51 __MODEL__"
#elif ((__MODEL__) == 1)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - LZO_ARCH_MCS51 __MODEL__"
#endif
#elif (LZO_ARCH_CRAY_PVP)
#  define LZO_MM_PVP            1
#else
#  define LZO_MM_FLAT           1
#endif
#if (LZO_MM_COMPACT)
#  define LZO_INFO_MM           "compact"
#elif (LZO_MM_FLAT)
#  define LZO_INFO_MM           "flat"
#elif (LZO_MM_HUGE)
#  define LZO_INFO_MM           "huge"
#elif (LZO_MM_LARGE)
#  define LZO_INFO_MM           "large"
#elif (LZO_MM_MEDIUM)
#  define LZO_INFO_MM           "medium"
#elif (LZO_MM_PVP)
#  define LZO_INFO_MM           "pvp"
#elif (LZO_MM_SMALL)
#  define LZO_INFO_MM           "small"
#elif (LZO_MM_TINY)
#  define LZO_INFO_MM           "tiny"
#else
#  error "unknown memory model"
#endif
#endif
#if !defined(__lzo_gnuc_extension__)
#if (LZO_CC_GNUC >= 0x020800ul)
#  define __lzo_gnuc_extension__    __extension__
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_gnuc_extension__    __extension__
#elif (LZO_CC_IBMC >= 600)
#  define __lzo_gnuc_extension__    __extension__
#else
#endif
#endif
#if !defined(__lzo_gnuc_extension__)
#  define __lzo_gnuc_extension__    /*empty*/
#endif
#if !defined(lzo_has_builtin)
#if (LZO_CC_CLANG) && defined(__has_builtin)
#  define lzo_has_builtin           __has_builtin
#endif
#endif
#if !defined(lzo_has_builtin)
#  define lzo_has_builtin(x)        0
#endif
#if !defined(lzo_has_attribute)
#if (LZO_CC_CLANG) && defined(__has_attribute)
#  define lzo_has_attribute         __has_attribute
#endif
#endif
#if !defined(lzo_has_attribute)
#  define lzo_has_attribute(x)      0
#endif
#if !defined(lzo_has_declspec_attribute)
#if (LZO_CC_CLANG) && defined(__has_declspec_attribute)
#  define lzo_has_declspec_attribute        __has_declspec_attribute
#endif
#endif
#if !defined(lzo_has_declspec_attribute)
#  define lzo_has_declspec_attribute(x)     0
#endif
#if !defined(lzo_has_feature)
#if (LZO_CC_CLANG) && defined(__has_feature)
#  define lzo_has_feature         __has_feature
#endif
#endif
#if !defined(lzo_has_feature)
#  define lzo_has_feature(x)        0
#endif
#if !defined(lzo_has_extension)
#if (LZO_CC_CLANG) && defined(__has_extension)
#  define lzo_has_extension         __has_extension
#elif (LZO_CC_CLANG) && defined(__has_feature)
#  define lzo_has_extension         __has_feature
#endif
#endif
#if !defined(lzo_has_extension)
#  define lzo_has_extension         0
#endif
#if !defined(LZO_CFG_USE_NEW_STYLE_CASTS) && defined(__cplusplus) && 0
#  if (LZO_CC_GNUC && (LZO_CC_GNUC < 0x020800ul))
#    define LZO_CFG_USE_NEW_STYLE_CASTS 0
#  elif (LZO_CC_INTELC && (__INTEL_COMPILER < 1200))
#    define LZO_CFG_USE_NEW_STYLE_CASTS 0
#  else
#    define LZO_CFG_USE_NEW_STYLE_CASTS 1
#  endif
#endif
#if !defined(LZO_CFG_USE_NEW_STYLE_CASTS)
#  define LZO_CFG_USE_NEW_STYLE_CASTS 0
#endif
#if !defined(__cplusplus)
#  if defined(LZO_CFG_USE_NEW_STYLE_CASTS)
#    undef LZO_CFG_USE_NEW_STYLE_CASTS
#  endif
#  define LZO_CFG_USE_NEW_STYLE_CASTS 0
#endif
#if !defined(LZO_REINTERPRET_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_REINTERPRET_CAST(t,e)       (reinterpret_cast<t> (e))
#  endif
#endif
#if !defined(LZO_REINTERPRET_CAST)
#  define LZO_REINTERPRET_CAST(t,e)         ((t) (e))
#endif
#if !defined(LZO_STATIC_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_STATIC_CAST(t,e)            (static_cast<t> (e))
#  endif
#endif
#if !defined(LZO_STATIC_CAST)
#  define LZO_STATIC_CAST(t,e)              ((t) (e))
#endif
#if !defined(LZO_STATIC_CAST2)
#  define LZO_STATIC_CAST2(t1,t2,e)         LZO_STATIC_CAST(t1, LZO_STATIC_CAST(t2, e))
#endif
#if !defined(LZO_UNCONST_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_UNCONST_CAST(t,e)           (const_cast<t> (e))
#  elif (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_UNCONST_CAST(t,e)           ((t) (e))
#  elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNCONST_CAST(t,e)           ((t) ((void *) ((lzo_uintptr_t) ((const void *) (e)))))
#  endif
#endif
#if !defined(LZO_UNCONST_CAST)
#  define LZO_UNCONST_CAST(t,e)             ((t) ((void *) ((const void *) (e))))
#endif
#if !defined(LZO_UNCONST_VOLATILE_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_UNCONST_VOLATILE_CAST(t,e)  (const_cast<t> (e))
#  elif (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_UNCONST_VOLATILE_CAST(t,e)  ((t) (e))
#  elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNCONST_VOLATILE_CAST(t,e)  ((t) ((volatile void *) ((lzo_uintptr_t) ((volatile const void *) (e)))))
#  endif
#endif
#if !defined(LZO_UNCONST_VOLATILE_CAST)
#  define LZO_UNCONST_VOLATILE_CAST(t,e)    ((t) ((volatile void *) ((volatile const void *) (e))))
#endif
#if !defined(LZO_UNVOLATILE_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_UNVOLATILE_CAST(t,e)        (const_cast<t> (e))
#  elif (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_UNVOLATILE_CAST(t,e)        ((t) (e))
#  elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNVOLATILE_CAST(t,e)        ((t) ((void *) ((lzo_uintptr_t) ((volatile void *) (e)))))
#  endif
#endif
#if !defined(LZO_UNVOLATILE_CAST)
#  define LZO_UNVOLATILE_CAST(t,e)          ((t) ((void *) ((volatile void *) (e))))
#endif
#if !defined(LZO_UNVOLATILE_CONST_CAST)
#  if (LZO_CFG_USE_NEW_STYLE_CASTS)
#    define LZO_UNVOLATILE_CONST_CAST(t,e)  (const_cast<t> (e))
#  elif (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_UNVOLATILE_CONST_CAST(t,e)  ((t) (e))
#  elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNVOLATILE_CONST_CAST(t,e)  ((t) ((const void *) ((lzo_uintptr_t) ((volatile const void *) (e)))))
#  endif
#endif
#if !defined(LZO_UNVOLATILE_CONST_CAST)
#  define LZO_UNVOLATILE_CONST_CAST(t,e)    ((t) ((const void *) ((volatile const void *) (e))))
#endif
#if !defined(LZO_PCAST)
#  if (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_PCAST(t,e)                  ((t) (e))
#  endif
#endif
#if !defined(LZO_PCAST)
#  define LZO_PCAST(t,e)                    LZO_STATIC_CAST(t, LZO_STATIC_CAST(void *, e))
#endif
#if !defined(LZO_CCAST)
#  if (LZO_HAVE_MM_HUGE_PTR)
#    define LZO_CCAST(t,e)                  ((t) (e))
#  endif
#endif
#if !defined(LZO_CCAST)
#  define LZO_CCAST(t,e)                    LZO_STATIC_CAST(t, LZO_STATIC_CAST(const void *, e))
#endif
#if !defined(LZO_ICONV)
#  define LZO_ICONV(t,e)                    LZO_STATIC_CAST(t, e)
#endif
#if !defined(LZO_ICAST)
#  define LZO_ICAST(t,e)                    LZO_STATIC_CAST(t, e)
#endif
#if !defined(LZO_ITRUNC)
#  define LZO_ITRUNC(t,e)                   LZO_STATIC_CAST(t, e)
#endif
#if !defined(__lzo_cte)
#  if (LZO_CC_MSC || LZO_CC_WATCOMC)
#    define __lzo_cte(e)            ((void)0,(e))
#  elif 1
#    define __lzo_cte(e)            ((void)0,(e))
#  endif
#endif
#if !defined(__lzo_cte)
#  define __lzo_cte(e)              (e)
#endif
#if !defined(LZO_BLOCK_BEGIN)
#  define LZO_BLOCK_BEGIN           do {
#  define LZO_BLOCK_END             } while __lzo_cte(0)
#endif
#if !defined(LZO_UNUSED)
#  if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0600))
#    define LZO_UNUSED(var)         ((void) &var)
#  elif (LZO_CC_BORLANDC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PELLESC || LZO_CC_TURBOC)
#    define LZO_UNUSED(var)         if (&var) ; else
#  elif (LZO_CC_CLANG && (LZO_CC_CLANG >= 0x030200ul))
#    define LZO_UNUSED(var)         ((void) &var)
#  elif (LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNUSED(var)         ((void) var)
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_UNUSED(var)         if (&var) ; else
#  elif (LZO_CC_KEILC)
#    define LZO_UNUSED(var)         {extern int lzo_unused__[1-2*!(sizeof(var)>0)]; (void)lzo_unused__;}
#  elif (LZO_CC_PACIFICC)
#    define LZO_UNUSED(var)         ((void) sizeof(var))
#  elif (LZO_CC_WATCOMC) && defined(__cplusplus)
#    define LZO_UNUSED(var)         ((void) var)
#  else
#    define LZO_UNUSED(var)         ((void) &var)
#  endif
#endif
#if !defined(LZO_UNUSED_FUNC)
#  if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0600))
#    define LZO_UNUSED_FUNC(func)   ((void) func)
#  elif (LZO_CC_BORLANDC || LZO_CC_NDPC || LZO_CC_TURBOC)
#    define LZO_UNUSED_FUNC(func)   if (func) ; else
#  elif (LZO_CC_CLANG || LZO_CC_LLVM)
#    define LZO_UNUSED_FUNC(func)   ((void) &func)
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_UNUSED_FUNC(func)   if (func) ; else
#  elif (LZO_CC_MSC)
#    define LZO_UNUSED_FUNC(func)   ((void) &func)
#  elif (LZO_CC_KEILC || LZO_CC_PELLESC)
#    define LZO_UNUSED_FUNC(func)   {extern int lzo_unused__[1-2*!(sizeof((int)func)>0)]; (void)lzo_unused__;}
#  else
#    define LZO_UNUSED_FUNC(func)   ((void) func)
#  endif
#endif
#if !defined(LZO_UNUSED_LABEL)
#  if (LZO_CC_CLANG >= 0x020800ul)
#    define LZO_UNUSED_LABEL(l)     (__lzo_gnuc_extension__ ((void) ((const void *) &&l)))
#  elif (LZO_CC_ARMCC || LZO_CC_CLANG || LZO_CC_INTELC || LZO_CC_WATCOMC)
#    define LZO_UNUSED_LABEL(l)     if __lzo_cte(0) goto l
#  else
#    define LZO_UNUSED_LABEL(l)     switch (0) case 1:goto l
#  endif
#endif
#if !defined(LZO_DEFINE_UNINITIALIZED_VAR)
#  if 0
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var
#  elif 0 && (LZO_CC_GNUC)
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var = var
#  else
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var = init
#  endif
#endif
#if !defined(__lzo_inline)
#if (LZO_CC_TURBOC && (__TURBOC__ <= 0x0295))
#elif defined(__cplusplus)
#  define __lzo_inline          inline
#elif defined(__STDC_VERSION__) && (__STDC_VERSION__-0 >= 199901L)
#  define __lzo_inline          inline
#elif (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0550))
#  define __lzo_inline          __inline
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CILLY || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
#  define __lzo_inline          __inline__
#elif (LZO_CC_DMC)
#  define __lzo_inline          __inline
#elif (LZO_CC_GHS)
#  define __lzo_inline          __inline__
#elif (LZO_CC_IBMC >= 600)
#  define __lzo_inline          __inline__
#elif (LZO_CC_INTELC)
#  define __lzo_inline          __inline
#elif (LZO_CC_MWERKS && (__MWERKS__ >= 0x2405))
#  define __lzo_inline          __inline
#elif (LZO_CC_MSC && (_MSC_VER >= 900))
#  define __lzo_inline          __inline
#elif (LZO_CC_SUNPROC >= 0x5100)
#  define __lzo_inline          __inline__
#endif
#endif
#if defined(__lzo_inline)
#  ifndef __lzo_HAVE_inline
#  define __lzo_HAVE_inline 1
#  endif
#else
#  define __lzo_inline          /*empty*/
#endif
#if !defined(__lzo_forceinline)
#if (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_IBMC >= 700)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 450))
#  define __lzo_forceinline     __forceinline
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 800))
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200))
#  define __lzo_forceinline     __forceinline
#elif (LZO_CC_PGI >= 0x0d0a00ul)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_SUNPROC >= 0x5100)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#endif
#endif
#if defined(__lzo_forceinline)
#  ifndef __lzo_HAVE_forceinline
#  define __lzo_HAVE_forceinline 1
#  endif
#else
#  define __lzo_forceinline     __lzo_inline
#endif
#if !defined(__lzo_noinline)
#if 1 && (LZO_ARCH_I386) && (LZO_CC_GNUC >= 0x040000ul) && (LZO_CC_GNUC < 0x040003ul)
#  define __lzo_noinline        __attribute__((__noinline__,__used__))
#elif (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_IBMC >= 700)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 600))
#  define __lzo_noinline        __declspec(noinline)
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 800))
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_noinline        __declspec(noinline)
#elif (LZO_CC_MWERKS && (__MWERKS__ >= 0x3200) && (LZO_OS_WIN32 || LZO_OS_WIN64))
#  if defined(__cplusplus)
#  else
#    define __lzo_noinline      __declspec(noinline)
#  endif
#elif (LZO_CC_PGI >= 0x0d0a00ul)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_SUNPROC >= 0x5100)
#  define __lzo_noinline        __attribute__((__noinline__))
#endif
#endif
#if defined(__lzo_noinline)
#  ifndef __lzo_HAVE_noinline
#  define __lzo_HAVE_noinline 1
#  endif
#else
#  define __lzo_noinline        /*empty*/
#endif
#if (__lzo_HAVE_forceinline || __lzo_HAVE_noinline) && !(__lzo_HAVE_inline)
#  error "unexpected configuration - check your compiler defines"
#endif
#if !defined(__lzo_static_inline)
#if (LZO_CC_IBMC)
#  define __lzo_static_inline       __lzo_gnuc_extension__ static __lzo_inline
#endif
#endif
#if !defined(__lzo_static_inline)
#  define __lzo_static_inline       static __lzo_inline
#endif
#if !defined(__lzo_static_forceinline)
#if (LZO_CC_IBMC)
#  define __lzo_static_forceinline  __lzo_gnuc_extension__ static __lzo_forceinline
#endif
#endif
#if !defined(__lzo_static_forceinline)
#  define __lzo_static_forceinline  static __lzo_forceinline
#endif
#if !defined(__lzo_static_noinline)
#if (LZO_CC_IBMC)
#  define __lzo_static_noinline     __lzo_gnuc_extension__ static __lzo_noinline
#endif
#endif
#if !defined(__lzo_static_noinline)
#  define __lzo_static_noinline     static __lzo_noinline
#endif
#if !defined(__lzo_c99_extern_inline)
#if defined(__GNUC_GNU_INLINE__)
#  define __lzo_c99_extern_inline   __lzo_inline
#elif defined(__GNUC_STDC_INLINE__)
#  define __lzo_c99_extern_inline   extern __lzo_inline
#elif defined(__STDC_VERSION__) && (__STDC_VERSION__-0 >= 199901L)
#  define __lzo_c99_extern_inline   extern __lzo_inline
#endif
#if !defined(__lzo_c99_extern_inline) && (__lzo_HAVE_inline)
#  define __lzo_c99_extern_inline   __lzo_inline
#endif
#endif
#if defined(__lzo_c99_extern_inline)
#  ifndef __lzo_HAVE_c99_extern_inline
#  define __lzo_HAVE_c99_extern_inline 1
#  endif
#else
#  define __lzo_c99_extern_inline   /*empty*/
#endif
#if !defined(__lzo_may_alias)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_may_alias       __attribute__((__may_alias__))
#elif (LZO_CC_CLANG >= 0x020900ul)
#  define __lzo_may_alias       __attribute__((__may_alias__))
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 1210)) && 0
#  define __lzo_may_alias       __attribute__((__may_alias__))
#elif (LZO_CC_PGI >= 0x0d0a00ul) && 0
#  define __lzo_may_alias       __attribute__((__may_alias__))
#endif
#endif
#if defined(__lzo_may_alias)
#  ifndef __lzo_HAVE_may_alias
#  define __lzo_HAVE_may_alias 1
#  endif
#else
#  define __lzo_may_alias       /*empty*/
#endif
#if !defined(__lzo_noreturn)
#if (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_IBMC >= 700)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 450))
#  define __lzo_noreturn        __declspec(noreturn)
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 600))
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200))
#  define __lzo_noreturn        __declspec(noreturn)
#elif (LZO_CC_PGI >= 0x0d0a00ul)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#endif
#endif
#if defined(__lzo_noreturn)
#  ifndef __lzo_HAVE_noreturn
#  define __lzo_HAVE_noreturn 1
#  endif
#else
#  define __lzo_noreturn        /*empty*/
#endif
#if !defined(__lzo_nothrow)
#if (LZO_CC_GNUC >= 0x030300ul)
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 450)) && defined(__cplusplus)
#  define __lzo_nothrow         __declspec(nothrow)
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 900))
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200)) && defined(__cplusplus)
#  define __lzo_nothrow         __declspec(nothrow)
#endif
#endif
#if defined(__lzo_nothrow)
#  ifndef __lzo_HAVE_nothrow
#  define __lzo_HAVE_nothrow 1
#  endif
#else
#  define __lzo_nothrow         /*empty*/
#endif
#if !defined(__lzo_restrict)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_IBMC >= 800) && !defined(__cplusplus)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_IBMC >= 1210)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 600))
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 600))
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_MSC && (_MSC_VER >= 1400))
#  define __lzo_restrict        __restrict
#elif (LZO_CC_PGI >= 0x0d0a00ul)
#  define __lzo_restrict        __restrict__
#endif
#endif
#if defined(__lzo_restrict)
#  ifndef __lzo_HAVE_restrict
#  define __lzo_HAVE_restrict 1
#  endif
#else
#  define __lzo_restrict        /*empty*/
#endif
#if !defined(__lzo_alignof)
#if (LZO_CC_ARMCC || LZO_CC_CILLY || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
#  define __lzo_alignof(e)      __alignof__(e)
#elif (LZO_CC_GHS) && !defined(__cplusplus)
#  define __lzo_alignof(e)      __alignof__(e)
#elif (LZO_CC_IBMC >= 600)
#  define __lzo_alignof(e)      (__lzo_gnuc_extension__ __alignof__(e))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 700))
#  define __lzo_alignof(e)      __alignof__(e)
#elif (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_alignof(e)      __alignof(e)
#elif (LZO_CC_SUNPROC >= 0x5100)
#  define __lzo_alignof(e)      __alignof__(e)
#endif
#endif
#if defined(__lzo_alignof)
#  ifndef __lzo_HAVE_alignof
#  define __lzo_HAVE_alignof 1
#  endif
#endif
#if !defined(__lzo_struct_packed)
#if   (LZO_CC_CLANG && (LZO_CC_CLANG < 0x020800ul)) && defined(__cplusplus)
#elif (LZO_CC_GNUC && (LZO_CC_GNUC < 0x020700ul))
#elif (LZO_CC_GNUC && (LZO_CC_GNUC < 0x020800ul)) && defined(__cplusplus)
#elif (LZO_CC_PCC && (LZO_CC_PCC < 0x010100ul))
#elif (LZO_CC_SUNPROC && (LZO_CC_SUNPROC < 0x5110)) && !defined(__cplusplus)
#elif (LZO_CC_GNUC >= 0x030400ul) && !(LZO_CC_PCC_GNUC) && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
#  define __lzo_struct_packed(s)        struct s {
#  define __lzo_struct_packed_end()     } __attribute__((__gcc_struct__,__packed__));
#  define __lzo_struct_packed_ma_end()  } __lzo_may_alias __attribute__((__gcc_struct__,__packed__));
#elif (LZO_CC_ARMCC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_INTELC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || (LZO_CC_PGI >= 0x0d0a00ul) || (LZO_CC_SUNPROC >= 0x5100))
#  define __lzo_struct_packed(s)        struct s {
#  define __lzo_struct_packed_end()     } __attribute__((__packed__));
#  define __lzo_struct_packed_ma_end()  } __lzo_may_alias __attribute__((__packed__));
#elif (LZO_CC_IBMC >= 700)
#  define __lzo_struct_packed(s)        __lzo_gnuc_extension__ struct s {
#  define __lzo_struct_packed_end()     } __attribute__((__packed__));
#  define __lzo_struct_packed_ma_end()  } __lzo_may_alias __attribute__((__packed__));
#elif (LZO_CC_INTELC_MSC) || (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_struct_packed(s)        __pragma(pack(push,1)) struct s {
#  define __lzo_struct_packed_end()     } __pragma(pack(pop));
#elif (LZO_CC_WATCOMC && (__WATCOMC__ >= 900))
#  define __lzo_struct_packed(s)        _Packed struct s {
#  define __lzo_struct_packed_end()     };
#endif
#endif
#if defined(__lzo_struct_packed) && !defined(__lzo_struct_packed_ma)
#  define __lzo_struct_packed_ma(s)     __lzo_struct_packed(s)
#endif
#if defined(__lzo_struct_packed_end) && !defined(__lzo_struct_packed_ma_end)
#  define __lzo_struct_packed_ma_end()  __lzo_struct_packed_end()
#endif
#if !defined(__lzo_byte_struct)
#if defined(__lzo_struct_packed)
#  define __lzo_byte_struct(s,n)        __lzo_struct_packed(s) unsigned char a[n]; __lzo_struct_packed_end()
#  define __lzo_byte_struct_ma(s,n)     __lzo_struct_packed_ma(s) unsigned char a[n]; __lzo_struct_packed_ma_end()
#elif (LZO_CC_CILLY || LZO_CC_CLANG || LZO_CC_PGI || (LZO_CC_SUNPROC >= 0x5100))
#  define __lzo_byte_struct(s,n)        struct s { unsigned char a[n]; } __attribute__((__packed__));
#  define __lzo_byte_struct_ma(s,n)     struct s { unsigned char a[n]; } __lzo_may_alias __attribute__((__packed__));
#endif
#endif
#if defined(__lzo_byte_struct) &&  !defined(__lzo_byte_struct_ma)
#  define __lzo_byte_struct_ma(s,n)     __lzo_byte_struct(s,n)
#endif
#if !defined(__lzo_struct_align16) && (__lzo_HAVE_alignof)
#if (LZO_CC_GNUC && (LZO_CC_GNUC < 0x030000ul))
#elif (LZO_CC_CLANG && (LZO_CC_CLANG < 0x020800ul)) && defined(__cplusplus)
#elif (LZO_CC_CILLY || LZO_CC_PCC)
#elif (LZO_CC_INTELC_MSC) || (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_struct_align16(s)       struct __declspec(align(16)) s {
#  define __lzo_struct_align16_end()    };
#  define __lzo_struct_align32(s)       struct __declspec(align(32)) s {
#  define __lzo_struct_align32_end()    };
#  define __lzo_struct_align64(s)       struct __declspec(align(64)) s {
#  define __lzo_struct_align64_end()    };
#elif (LZO_CC_ARMCC || LZO_CC_CLANG || LZO_CC_GNUC || (LZO_CC_IBMC >= 700) || LZO_CC_INTELC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_struct_align16(s)       struct s {
#  define __lzo_struct_align16_end()    } __attribute__((__aligned__(16)));
#  define __lzo_struct_align32(s)       struct s {
#  define __lzo_struct_align32_end()    } __attribute__((__aligned__(32)));
#  define __lzo_struct_align64(s)       struct s {
#  define __lzo_struct_align64_end()    } __attribute__((__aligned__(64)));
#endif
#endif
#if !defined(__lzo_union_um)
#if   (LZO_CC_CLANG && (LZO_CC_CLANG < 0x020800ul)) && defined(__cplusplus)
#elif (LZO_CC_GNUC && (LZO_CC_GNUC < 0x020700ul))
#elif (LZO_CC_GNUC && (LZO_CC_GNUC < 0x020800ul)) && defined(__cplusplus)
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER < 810))
#elif (LZO_CC_PCC && (LZO_CC_PCC < 0x010100ul))
#elif (LZO_CC_SUNPROC && (LZO_CC_SUNPROC < 0x5110)) && !defined(__cplusplus)
#elif (LZO_CC_ARMCC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_INTELC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || (LZO_CC_PGI >= 0x0d0a00ul) || (LZO_CC_SUNPROC >= 0x5100))
#  define __lzo_union_am(s)             union s {
#  define __lzo_union_am_end()          } __lzo_may_alias;
#  define __lzo_union_um(s)             union s {
#  define __lzo_union_um_end()          } __lzo_may_alias __attribute__((__packed__));
#elif (LZO_CC_IBMC >= 700)
#  define __lzo_union_am(s)             __lzo_gnuc_extension__ union s {
#  define __lzo_union_am_end()          } __lzo_may_alias;
#  define __lzo_union_um(s)             __lzo_gnuc_extension__ union s {
#  define __lzo_union_um_end()          } __lzo_may_alias __attribute__((__packed__));
#elif (LZO_CC_INTELC_MSC) || (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_union_um(s)             __pragma(pack(push,1)) union s {
#  define __lzo_union_um_end()          } __pragma(pack(pop));
#elif (LZO_CC_WATCOMC && (__WATCOMC__ >= 900))
#  define __lzo_union_um(s)             _Packed union s {
#  define __lzo_union_um_end()          };
#endif
#endif
#if !defined(__lzo_union_am)
#  define __lzo_union_am(s)             union s {
#  define __lzo_union_am_end()          };
#endif
#if !defined(__lzo_constructor)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_constructor     __attribute__((__constructor__,__used__))
#elif (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_constructor     __attribute__((__constructor__))
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 800))
#  define __lzo_constructor     __attribute__((__constructor__,__used__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_constructor     __attribute__((__constructor__))
#endif
#endif
#if defined(__lzo_constructor)
#  ifndef __lzo_HAVE_constructor
#  define __lzo_HAVE_constructor 1
#  endif
#endif
#if !defined(__lzo_destructor)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_destructor      __attribute__((__destructor__,__used__))
#elif (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_destructor      __attribute__((__destructor__))
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 800))
#  define __lzo_destructor      __attribute__((__destructor__,__used__))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_destructor      __attribute__((__destructor__))
#endif
#endif
#if defined(__lzo_destructor)
#  ifndef __lzo_HAVE_destructor
#  define __lzo_HAVE_destructor 1
#  endif
#endif
#if (__lzo_HAVE_destructor) && !(__lzo_HAVE_constructor)
#  error "unexpected configuration - check your compiler defines"
#endif
#if !defined(__lzo_likely) && !defined(__lzo_unlikely)
#if (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#elif (LZO_CC_IBMC >= 1010)
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 800))
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#endif
#endif
#if defined(__lzo_likely)
#  ifndef __lzo_HAVE_likely
#  define __lzo_HAVE_likely 1
#  endif
#else
#  define __lzo_likely(e)           (e)
#endif
#if defined(__lzo_very_likely)
#  ifndef __lzo_HAVE_very_likely
#  define __lzo_HAVE_very_likely 1
#  endif
#else
#  define __lzo_very_likely(e)      __lzo_likely(e)
#endif
#if defined(__lzo_unlikely)
#  ifndef __lzo_HAVE_unlikely
#  define __lzo_HAVE_unlikely 1
#  endif
#else
#  define __lzo_unlikely(e)         (e)
#endif
#if defined(__lzo_very_unlikely)
#  ifndef __lzo_HAVE_very_unlikely
#  define __lzo_HAVE_very_unlikely 1
#  endif
#else
#  define __lzo_very_unlikely(e)    __lzo_unlikely(e)
#endif
#if !defined(__lzo_loop_forever)
#  if (LZO_CC_IBMC)
#    define __lzo_loop_forever()    LZO_BLOCK_BEGIN for (;;) { ; } LZO_BLOCK_END
#  else
#    define __lzo_loop_forever()    do { ; } while __lzo_cte(1)
#  endif
#endif
#if !defined(__lzo_unreachable)
#if (LZO_CC_CLANG && (LZO_CC_CLANG >= 0x020800ul)) && lzo_has_builtin(__builtin_unreachable)
#  define __lzo_unreachable()       __builtin_unreachable();
#elif (LZO_CC_GNUC >= 0x040500ul)
#  define __lzo_unreachable()       __builtin_unreachable();
#elif (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 1300)) && 1
#  define __lzo_unreachable()       __builtin_unreachable();
#endif
#endif
#if defined(__lzo_unreachable)
#  ifndef __lzo_HAVE_unreachable
#  define __lzo_HAVE_unreachable 1
#  endif
#else
#  if 0
#  define __lzo_unreachable()       ((void)0);
#  else
#  define __lzo_unreachable()       __lzo_loop_forever();
#  endif
#endif
#if !defined(lzo_unused_funcs_impl)
#  if 1 && (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || (LZO_CC_GNUC >= 0x020700ul) || LZO_CC_INTELC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
#    define lzo_unused_funcs_impl(r,f)  static r __attribute__((__unused__)) f
#  elif 1 && (LZO_CC_BORLANDC || LZO_CC_GNUC)
#    define lzo_unused_funcs_impl(r,f)  static r f
#  else
#    define lzo_unused_funcs_impl(r,f)  __lzo_static_forceinline r f
#  endif
#endif
#ifndef __LZO_CTA_NAME
#if (LZO_CFG_USE_COUNTER)
#  define __LZO_CTA_NAME(a)         LZO_PP_ECONCAT2(a,__COUNTER__)
#else
#  define __LZO_CTA_NAME(a)         LZO_PP_ECONCAT2(a,__LINE__)
#endif
#endif
#if !defined(LZO_COMPILE_TIME_ASSERT_HEADER)
#  if (LZO_CC_AZTECC || LZO_CC_ZORTECHC)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN extern int __LZO_CTA_NAME(lzo_cta__)[1-!(e)]; LZO_EXTERN_C_END
#  elif (LZO_CC_DMC || LZO_CC_SYMANTECC)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN extern int __LZO_CTA_NAME(lzo_cta__)[1u-2*!(e)]; LZO_EXTERN_C_END
#  elif (LZO_CC_TURBOC && (__TURBOC__ == 0x0295))
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN extern int __LZO_CTA_NAME(lzo_cta__)[1-!(e)]; LZO_EXTERN_C_END
#  elif (LZO_CC_CLANG && (LZO_CC_CLANG < 0x020900ul)) && defined(__cplusplus)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN int __LZO_CTA_NAME(lzo_cta_f__)(int [1-2*!(e)]); LZO_EXTERN_C_END
#  elif (LZO_CC_GNUC) && defined(__CHECKER__) && defined(__SPARSE_CHECKER__)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN enum {__LZO_CTA_NAME(lzo_cta_e__)=1/!!(e)} __attribute__((__unused__)); LZO_EXTERN_C_END
#  else
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  LZO_EXTERN_C_BEGIN extern int __LZO_CTA_NAME(lzo_cta__)[1-2*!(e)]; LZO_EXTERN_C_END
#  endif
#endif
#if !defined(LZO_COMPILE_TIME_ASSERT)
#  if (LZO_CC_AZTECC)
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __LZO_CTA_NAME(lzo_cta_t__)[1-!(e)];}
#  elif (LZO_CC_CLANG && (LZO_CC_CLANG >= 0x030000ul))
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __LZO_CTA_NAME(lzo_cta_t__)[1-2*!(e)] __attribute__((__unused__));}
#  elif (LZO_CC_DMC || LZO_CC_PACIFICC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  elif (LZO_CC_GNUC) && defined(__CHECKER__) && defined(__SPARSE_CHECKER__)
#    define LZO_COMPILE_TIME_ASSERT(e)  {(void) (0/!!(e));}
#  elif (LZO_CC_GNUC >= 0x040700ul) && (LZO_CFG_USE_COUNTER) && defined(__cplusplus)
#    define LZO_COMPILE_TIME_ASSERT(e)  {enum {__LZO_CTA_NAME(lzo_cta_e__)=1/!!(e)} __attribute__((__unused__));}
#  elif (LZO_CC_GNUC >= 0x040700ul)
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __LZO_CTA_NAME(lzo_cta_t__)[1-2*!(e)] __attribute__((__unused__));}
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  elif (LZO_CC_TURBOC && (__TURBOC__ == 0x0295))
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  else
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __LZO_CTA_NAME(lzo_cta_t__)[1-2*!(e)];}
#  endif
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(1 == 1)
#if defined(__cplusplus)
extern "C" { LZO_COMPILE_TIME_ASSERT_HEADER(2 == 2) }
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(3 == 3)
#if (LZO_ARCH_I086 || LZO_ARCH_I386) && (LZO_OS_DOS16 || LZO_OS_DOS32 || LZO_OS_OS2 || LZO_OS_OS216 || LZO_OS_WIN16 || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (LZO_CC_GNUC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PACIFICC)
#  elif (LZO_CC_DMC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
#    define __lzo_cdecl                 __cdecl
#    define __lzo_cdecl_atexit          /*empty*/
#    define __lzo_cdecl_main            __cdecl
#    if (LZO_OS_OS2 && (LZO_CC_DMC || LZO_CC_SYMANTECC))
#      define __lzo_cdecl_qsort         __pascal
#    elif (LZO_OS_OS2 && (LZO_CC_ZORTECHC))
#      define __lzo_cdecl_qsort         _stdcall
#    else
#      define __lzo_cdecl_qsort         __cdecl
#    endif
#  elif (LZO_CC_WATCOMC)
#    define __lzo_cdecl                 __cdecl
#  else
#    define __lzo_cdecl                 __cdecl
#    define __lzo_cdecl_atexit          __cdecl
#    define __lzo_cdecl_main            __cdecl
#    define __lzo_cdecl_qsort           __cdecl
#  endif
#  if (LZO_CC_GNUC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PACIFICC || LZO_CC_WATCOMC)
#  elif (LZO_OS_OS2 && (LZO_CC_DMC || LZO_CC_SYMANTECC))
#    define __lzo_cdecl_sighandler      __pascal
#  elif (LZO_OS_OS2 && (LZO_CC_ZORTECHC))
#    define __lzo_cdecl_sighandler      _stdcall
#  elif (LZO_CC_MSC && (_MSC_VER >= 1400)) && defined(_M_CEE_PURE)
#    define __lzo_cdecl_sighandler      __clrcall
#  elif (LZO_CC_MSC && (_MSC_VER >= 600 && _MSC_VER < 700))
#    if defined(_DLL)
#      define __lzo_cdecl_sighandler    _far _cdecl _loadds
#    elif defined(_MT)
#      define __lzo_cdecl_sighandler    _far _cdecl
#    else
#      define __lzo_cdecl_sighandler    _cdecl
#    endif
#  else
#    define __lzo_cdecl_sighandler      __cdecl
#  endif
#elif (LZO_ARCH_I386) && (LZO_CC_WATCOMC)
#  define __lzo_cdecl                   __cdecl
#elif (LZO_ARCH_M68K && LZO_OS_TOS && (LZO_CC_PUREC || LZO_CC_TURBOC))
#  define __lzo_cdecl                   cdecl
#endif
#if !defined(__lzo_cdecl)
#  define __lzo_cdecl                   /*empty*/
#endif
#if !defined(__lzo_cdecl_atexit)
#  define __lzo_cdecl_atexit            /*empty*/
#endif
#if !defined(__lzo_cdecl_main)
#  define __lzo_cdecl_main              /*empty*/
#endif
#if !defined(__lzo_cdecl_qsort)
#  define __lzo_cdecl_qsort             /*empty*/
#endif
#if !defined(__lzo_cdecl_sighandler)
#  define __lzo_cdecl_sighandler        /*empty*/
#endif
#if !defined(__lzo_cdecl_va)
#  define __lzo_cdecl_va                __lzo_cdecl
#endif
#if !(LZO_CFG_NO_WINDOWS_H)
#if !defined(LZO_HAVE_WINDOWS_H)
#if (LZO_OS_CYGWIN || (LZO_OS_EMX && defined(__RSXNT__)) || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (LZO_CC_WATCOMC && (__WATCOMC__ < 1000))
#  elif ((LZO_OS_WIN32 && defined(__PW32__)) && (LZO_CC_GNUC && (LZO_CC_GNUC < 0x030000ul)))
#  elif ((LZO_OS_CYGWIN || defined(__MINGW32__)) && (LZO_CC_GNUC && (LZO_CC_GNUC < 0x025f00ul)))
#  else
#    define LZO_HAVE_WINDOWS_H 1
#  endif
#endif
#endif
#endif
#ifndef LZO_SIZEOF_SHORT
#if defined(SIZEOF_SHORT)
#  define LZO_SIZEOF_SHORT          (SIZEOF_SHORT)
#elif defined(__SIZEOF_SHORT__)
#  define LZO_SIZEOF_SHORT          (__SIZEOF_SHORT__)
#endif
#endif
#ifndef LZO_SIZEOF_INT
#if defined(SIZEOF_INT)
#  define LZO_SIZEOF_INT            (SIZEOF_INT)
#elif defined(__SIZEOF_INT__)
#  define LZO_SIZEOF_INT            (__SIZEOF_INT__)
#endif
#endif
#ifndef LZO_SIZEOF_LONG
#if defined(SIZEOF_LONG)
#  define LZO_SIZEOF_LONG           (SIZEOF_LONG)
#elif defined(__SIZEOF_LONG__)
#  define LZO_SIZEOF_LONG           (__SIZEOF_LONG__)
#endif
#endif
#ifndef LZO_SIZEOF_LONG_LONG
#if defined(SIZEOF_LONG_LONG)
#  define LZO_SIZEOF_LONG_LONG      (SIZEOF_LONG_LONG)
#elif defined(__SIZEOF_LONG_LONG__)
#  define LZO_SIZEOF_LONG_LONG      (__SIZEOF_LONG_LONG__)
#endif
#endif
#ifndef LZO_SIZEOF___INT16
#if defined(SIZEOF___INT16)
#  define LZO_SIZEOF___INT16        (SIZEOF___INT16)
#endif
#endif
#ifndef LZO_SIZEOF___INT32
#if defined(SIZEOF___INT32)
#  define LZO_SIZEOF___INT32        (SIZEOF___INT32)
#endif
#endif
#ifndef LZO_SIZEOF___INT64
#if defined(SIZEOF___INT64)
#  define LZO_SIZEOF___INT64        (SIZEOF___INT64)
#endif
#endif
#ifndef LZO_SIZEOF_VOID_P
#if defined(SIZEOF_VOID_P)
#  define LZO_SIZEOF_VOID_P         (SIZEOF_VOID_P)
#elif defined(__SIZEOF_POINTER__)
#  define LZO_SIZEOF_VOID_P         (__SIZEOF_POINTER__)
#endif
#endif
#ifndef LZO_SIZEOF_SIZE_T
#if defined(SIZEOF_SIZE_T)
#  define LZO_SIZEOF_SIZE_T         (SIZEOF_SIZE_T)
#elif defined(__SIZEOF_SIZE_T__)
#  define LZO_SIZEOF_SIZE_T         (__SIZEOF_SIZE_T__)
#endif
#endif
#ifndef LZO_SIZEOF_PTRDIFF_T
#if defined(SIZEOF_PTRDIFF_T)
#  define LZO_SIZEOF_PTRDIFF_T      (SIZEOF_PTRDIFF_T)
#elif defined(__SIZEOF_PTRDIFF_T__)
#  define LZO_SIZEOF_PTRDIFF_T      (__SIZEOF_PTRDIFF_T__)
#endif
#endif
#define __LZO_LSR(x,b)    (((x)+0ul) >> (b))
#if !defined(LZO_SIZEOF_SHORT)
#  if (LZO_ARCH_CRAY_PVP)
#    define LZO_SIZEOF_SHORT        8
#  elif (USHRT_MAX == LZO_0xffffL)
#    define LZO_SIZEOF_SHORT        2
#  elif (__LZO_LSR(USHRT_MAX,7) == 1)
#    define LZO_SIZEOF_SHORT        1
#  elif (__LZO_LSR(USHRT_MAX,15) == 1)
#    define LZO_SIZEOF_SHORT        2
#  elif (__LZO_LSR(USHRT_MAX,31) == 1)
#    define LZO_SIZEOF_SHORT        4
#  elif (__LZO_LSR(USHRT_MAX,63) == 1)
#    define LZO_SIZEOF_SHORT        8
#  elif (__LZO_LSR(USHRT_MAX,127) == 1)
#    define LZO_SIZEOF_SHORT        16
#  else
#    error "LZO_SIZEOF_SHORT"
#  endif
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_SHORT == sizeof(short))
#if !defined(LZO_SIZEOF_INT)
#  if (LZO_ARCH_CRAY_PVP)
#    define LZO_SIZEOF_INT          8
#  elif (UINT_MAX == LZO_0xffffL)
#    define LZO_SIZEOF_INT          2
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_SIZEOF_INT          4
#  elif (__LZO_LSR(UINT_MAX,7) == 1)
#    define LZO_SIZEOF_INT          1
#  elif (__LZO_LSR(UINT_MAX,15) == 1)
#    define LZO_SIZEOF_INT          2
#  elif (__LZO_LSR(UINT_MAX,31) == 1)
#    define LZO_SIZEOF_INT          4
#  elif (__LZO_LSR(UINT_MAX,63) == 1)
#    define LZO_SIZEOF_INT          8
#  elif (__LZO_LSR(UINT_MAX,127) == 1)
#    define LZO_SIZEOF_INT          16
#  else
#    error "LZO_SIZEOF_INT"
#  endif
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_INT == sizeof(int))
#if !defined(LZO_SIZEOF_LONG)
#  if (ULONG_MAX == LZO_0xffffffffL)
#    define LZO_SIZEOF_LONG         4
#  elif (__LZO_LSR(ULONG_MAX,7) == 1)
#    define LZO_SIZEOF_LONG         1
#  elif (__LZO_LSR(ULONG_MAX,15) == 1)
#    define LZO_SIZEOF_LONG         2
#  elif (__LZO_LSR(ULONG_MAX,31) == 1)
#    define LZO_SIZEOF_LONG         4
#  elif (__LZO_LSR(ULONG_MAX,39) == 1)
#    define LZO_SIZEOF_LONG         5
#  elif (__LZO_LSR(ULONG_MAX,63) == 1)
#    define LZO_SIZEOF_LONG         8
#  elif (__LZO_LSR(ULONG_MAX,127) == 1)
#    define LZO_SIZEOF_LONG         16
#  else
#    error "LZO_SIZEOF_LONG"
#  endif
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_LONG == sizeof(long))
#if !defined(LZO_SIZEOF_LONG_LONG) && !defined(LZO_SIZEOF___INT64)
#if (LZO_SIZEOF_LONG > 0 && LZO_SIZEOF_LONG < 8)
#  if defined(__LONG_MAX__) && defined(__LONG_LONG_MAX__)
#    if (LZO_CC_GNUC >= 0x030300ul)
#      if ((__LONG_MAX__-0) == (__LONG_LONG_MAX__-0))
#        define LZO_SIZEOF_LONG_LONG      LZO_SIZEOF_LONG
#      elif (__LZO_LSR(__LONG_LONG_MAX__,30) == 1)
#        define LZO_SIZEOF_LONG_LONG      4
#      endif
#    endif
#  endif
#endif
#endif
#if !defined(LZO_SIZEOF_LONG_LONG) && !defined(LZO_SIZEOF___INT64)
#if (LZO_SIZEOF_LONG > 0 && LZO_SIZEOF_LONG < 8)
#if (LZO_ARCH_I086 && LZO_CC_DMC)
#elif (LZO_CC_CILLY) && defined(__GNUC__)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define LZO_SIZEOF_LONG_LONG      8
#elif ((LZO_OS_WIN32 || LZO_OS_WIN64 || defined(_WIN32)) && LZO_CC_MSC && (_MSC_VER >= 1400))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_OS_WIN64 || defined(_WIN64))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_DMC))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_SYMANTECC && (__SC__ >= 0x700)))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_INTELC && defined(__linux__)))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_MWERKS || LZO_CC_PELLESC || LZO_CC_PGI || LZO_CC_SUNPROC))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_INTELC || LZO_CC_MSC))
#  define LZO_SIZEOF___INT64        8
#elif ((LZO_OS_WIN32 || defined(_WIN32)) && (LZO_CC_MSC))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0520)))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_WATCOMC && (__WATCOMC__ >= 1100)))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_CC_GHS && defined(__LLONG_BIT) && ((__LLONG_BIT-0) == 64))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_CC_WATCOMC && defined(_INTEGRAL_MAX_BITS) && ((_INTEGRAL_MAX_BITS-0) == 64))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_OS_OS400 || defined(__OS400__)) && defined(__LLP64_IFC__)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (defined(__vms) || defined(__VMS)) && ((__INITIAL_POINTER_SIZE-0) == 64)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_CC_SDCC) && (LZO_SIZEOF_INT == 2)
#elif 1 && defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)
#  define LZO_SIZEOF_LONG_LONG      8
#endif
#endif
#endif
#if defined(__cplusplus) && (LZO_CC_GNUC)
#  if (LZO_CC_GNUC < 0x020800ul)
#    undef LZO_SIZEOF_LONG_LONG
#  endif
#endif
#if (LZO_CFG_NO_LONG_LONG)
#  undef LZO_SIZEOF_LONG_LONG
#elif defined(__NO_LONG_LONG)
#  undef LZO_SIZEOF_LONG_LONG
#elif defined(_NO_LONGLONG)
#  undef LZO_SIZEOF_LONG_LONG
#endif
#if !defined(LZO_WORDSIZE)
#if (LZO_ARCH_ALPHA)
#  define LZO_WORDSIZE              8
#elif (LZO_ARCH_AMD64)
#  define LZO_WORDSIZE              8
#elif (LZO_ARCH_AVR)
#  define LZO_WORDSIZE              1
#elif (LZO_ARCH_H8300)
#  if defined(__NORMAL_MODE__)
#    define LZO_WORDSIZE            4
#  elif defined(__H8300H__) || defined(__H8300S__) || defined(__H8300SX__)
#    define LZO_WORDSIZE            4
#  else
#    define LZO_WORDSIZE            2
#  endif
#elif (LZO_ARCH_I086)
#  define LZO_WORDSIZE              2
#elif (LZO_ARCH_IA64)
#  define LZO_WORDSIZE              8
#elif (LZO_ARCH_M16C)
#  define LZO_WORDSIZE              2
#elif (LZO_ARCH_SPU)
#  define LZO_WORDSIZE              4
#elif (LZO_ARCH_Z80)
#  define LZO_WORDSIZE              1
#elif (LZO_SIZEOF_LONG == 8) && ((defined(__mips__) && defined(__R5900__)) || defined(__MIPS_PSX2__))
#  define LZO_WORDSIZE              8
#elif (LZO_OS_OS400 || defined(__OS400__))
#  define LZO_WORDSIZE              8
#elif (defined(__vms) || defined(__VMS)) && (__INITIAL_POINTER_SIZE+0 == 64)
#  define LZO_WORDSIZE              8
#endif
#endif
#if !defined(LZO_SIZEOF_VOID_P)
#if defined(__ILP32__) || defined(__ILP32) || defined(_ILP32)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(int)  == 4)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(long) == 4)
#  define LZO_SIZEOF_VOID_P         4
#elif defined(__ILP64__) || defined(__ILP64) || defined(_ILP64)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(int)  == 8)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(long) == 8)
#  define LZO_SIZEOF_VOID_P         8
#elif defined(__LLP64__) || defined(__LLP64) || defined(_LLP64) || defined(_WIN64)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(long) == 4)
#  define LZO_SIZEOF_VOID_P         8
#elif defined(__LP64__) || defined(__LP64) || defined(_LP64)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(long) == 8)
#  define LZO_SIZEOF_VOID_P         8
#elif (LZO_ARCH_AVR)
#  define LZO_SIZEOF_VOID_P         2
#elif (LZO_ARCH_C166 || LZO_ARCH_MCS51 || LZO_ARCH_MCS251 || LZO_ARCH_MSP430)
#  define LZO_SIZEOF_VOID_P         2
#elif (LZO_ARCH_H8300)
#  if defined(__NORMAL_MODE__)
#    define LZO_SIZEOF_VOID_P       2
#  elif defined(__H8300H__) || defined(__H8300S__) || defined(__H8300SX__)
#    define LZO_SIZEOF_VOID_P       4
#  else
#    define LZO_SIZEOF_VOID_P       2
#  endif
#  if (LZO_CC_GNUC && (LZO_CC_GNUC < 0x040000ul)) && (LZO_SIZEOF_INT == 4)
#    define LZO_SIZEOF_SIZE_T       LZO_SIZEOF_INT
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_INT
#  endif
#elif (LZO_ARCH_I086)
#  if (LZO_MM_TINY || LZO_MM_SMALL || LZO_MM_MEDIUM)
#    define LZO_SIZEOF_VOID_P       2
#  elif (LZO_MM_COMPACT || LZO_MM_LARGE || LZO_MM_HUGE)
#    define LZO_SIZEOF_VOID_P       4
#  else
#    error "invalid LZO_ARCH_I086 memory model"
#  endif
#elif (LZO_ARCH_M16C)
#  if defined(__m32c_cpu__) || defined(__m32cm_cpu__)
#    define LZO_SIZEOF_VOID_P       4
#  else
#    define LZO_SIZEOF_VOID_P       2
#  endif
#elif (LZO_ARCH_SPU)
#  define LZO_SIZEOF_VOID_P         4
#elif (LZO_ARCH_Z80)
#  define LZO_SIZEOF_VOID_P         2
#elif (LZO_SIZEOF_LONG == 8) && ((defined(__mips__) && defined(__R5900__)) || defined(__MIPS_PSX2__))
#  define LZO_SIZEOF_VOID_P         4
#elif (LZO_OS_OS400 || defined(__OS400__))
#  if defined(__LLP64_IFC__)
#    define LZO_SIZEOF_VOID_P       8
#    define LZO_SIZEOF_SIZE_T       LZO_SIZEOF_LONG
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_LONG
#  else
#    define LZO_SIZEOF_VOID_P       16
#    define LZO_SIZEOF_SIZE_T       LZO_SIZEOF_LONG
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_LONG
#  endif
#elif (defined(__vms) || defined(__VMS)) && (__INITIAL_POINTER_SIZE+0 == 64)
#  define LZO_SIZEOF_VOID_P         8
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_LONG
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_LONG
#endif
#endif
#if !defined(LZO_SIZEOF_VOID_P)
#  define LZO_SIZEOF_VOID_P         LZO_SIZEOF_LONG
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_VOID_P == sizeof(void *))
#if !defined(LZO_SIZEOF_SIZE_T)
#if (LZO_ARCH_I086 || LZO_ARCH_M16C)
#  define LZO_SIZEOF_SIZE_T         2
#endif
#endif
#if !defined(LZO_SIZEOF_SIZE_T)
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_VOID_P
#endif
#if defined(offsetof)
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_SIZE_T == sizeof(size_t))
#endif
#if !defined(LZO_SIZEOF_PTRDIFF_T)
#if (LZO_ARCH_I086)
#  if (LZO_MM_TINY || LZO_MM_SMALL || LZO_MM_MEDIUM || LZO_MM_HUGE)
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_VOID_P
#  elif (LZO_MM_COMPACT || LZO_MM_LARGE)
#    if (LZO_CC_BORLANDC || LZO_CC_TURBOC)
#      define LZO_SIZEOF_PTRDIFF_T  4
#    else
#      define LZO_SIZEOF_PTRDIFF_T  2
#    endif
#  else
#    error "invalid LZO_ARCH_I086 memory model"
#  endif
#endif
#endif
#if !defined(LZO_SIZEOF_PTRDIFF_T)
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_SIZE_T
#endif
#if defined(offsetof)
LZO_COMPILE_TIME_ASSERT_HEADER(LZO_SIZEOF_PTRDIFF_T == sizeof(ptrdiff_t))
#endif
#if !defined(LZO_WORDSIZE)
#  define LZO_WORDSIZE              LZO_SIZEOF_VOID_P
#endif
#if (LZO_ABI_NEUTRAL_ENDIAN)
#  undef LZO_ABI_BIG_ENDIAN
#  undef LZO_ABI_LITTLE_ENDIAN
#elif !(LZO_ABI_BIG_ENDIAN) && !(LZO_ABI_LITTLE_ENDIAN)
#if (LZO_ARCH_ALPHA) && (LZO_ARCH_CRAY_MPP)
#  define LZO_ABI_BIG_ENDIAN        1
#elif (LZO_ARCH_IA64) && (LZO_OS_POSIX_LINUX || LZO_OS_WIN64)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif (LZO_ARCH_ALPHA || LZO_ARCH_AMD64 || LZO_ARCH_BLACKFIN || LZO_ARCH_CRIS || LZO_ARCH_I086 || LZO_ARCH_I386 || LZO_ARCH_MSP430)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif (LZO_ARCH_AVR32 || LZO_ARCH_M68K || LZO_ARCH_S390 || LZO_ARCH_SPU)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && defined(__IAR_SYSTEMS_ICC__) && defined(__LITTLE_ENDIAN__)
#  if (__LITTLE_ENDIAN__ == 1)
#    define LZO_ABI_LITTLE_ENDIAN   1
#  else
#    define LZO_ABI_BIG_ENDIAN      1
#  endif
#elif 1 && defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_ARM) && defined(__ARM_BIG_ENDIAN) && ((__ARM_BIG_ENDIAN)+0)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_ARM) && defined(__ARMEB__) && !defined(__ARMEL__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_ARM) && defined(__ARMEL__) && !defined(__ARMEB__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_ARM && LZO_CC_ARMCC_ARMCC)
#  if defined(__BIG_ENDIAN) && defined(__LITTLE_ENDIAN)
#    error "unexpected configuration - check your compiler defines"
#  elif defined(__BIG_ENDIAN)
#    define LZO_ABI_BIG_ENDIAN      1
#  else
#    define LZO_ABI_LITTLE_ENDIAN   1
#  endif
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_ARM64) && defined(__ARM_BIG_ENDIAN) && ((__ARM_BIG_ENDIAN)+0)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_ARM64) && defined(__AARCH64EB__) && !defined(__AARCH64EL__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_ARM64) && defined(__AARCH64EL__) && !defined(__AARCH64EB__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_MIPS) && defined(__MIPSEB__) && !defined(__MIPSEL__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_MIPS) && defined(__MIPSEL__) && !defined(__MIPSEB__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#endif
#endif
#if (LZO_ABI_BIG_ENDIAN) && (LZO_ABI_LITTLE_ENDIAN)
#  error "unexpected configuration - check your compiler defines"
#endif
#if (LZO_ABI_BIG_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "be"
#elif (LZO_ABI_LITTLE_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "le"
#elif (LZO_ABI_NEUTRAL_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "neutral"
#endif
#if (LZO_SIZEOF_INT == 1 && LZO_SIZEOF_LONG == 2 && LZO_SIZEOF_VOID_P == 2)
#  define LZO_ABI_I8LP16         1
#  define LZO_INFO_ABI_PM       "i8lp16"
#elif (LZO_SIZEOF_INT == 2 && LZO_SIZEOF_LONG == 2 && LZO_SIZEOF_VOID_P == 2)
#  define LZO_ABI_ILP16         1
#  define LZO_INFO_ABI_PM       "ilp16"
#elif (LZO_SIZEOF_INT == 2 && LZO_SIZEOF_LONG == 4 && LZO_SIZEOF_VOID_P == 4)
#  define LZO_ABI_LP32          1
#  define LZO_INFO_ABI_PM       "lp32"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 4 && LZO_SIZEOF_VOID_P == 4)
#  define LZO_ABI_ILP32         1
#  define LZO_INFO_ABI_PM       "ilp32"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 4 && LZO_SIZEOF_VOID_P == 8 && LZO_SIZEOF_SIZE_T == 8)
#  define LZO_ABI_LLP64         1
#  define LZO_INFO_ABI_PM       "llp64"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 8)
#  define LZO_ABI_LP64          1
#  define LZO_INFO_ABI_PM       "lp64"
#elif (LZO_SIZEOF_INT == 8 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 8)
#  define LZO_ABI_ILP64         1
#  define LZO_INFO_ABI_PM       "ilp64"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 4)
#  define LZO_ABI_IP32L64       1
#  define LZO_INFO_ABI_PM       "ip32l64"
#endif
#if 0
#elif !defined(__LZO_LIBC_OVERRIDE)
#if (LZO_LIBC_NAKED)
#  define LZO_INFO_LIBC         "naked"
#elif (LZO_LIBC_FREESTANDING)
#  define LZO_INFO_LIBC         "freestanding"
#elif (LZO_LIBC_MOSTLY_FREESTANDING)
#  define LZO_INFO_LIBC         "mfreestanding"
#elif (LZO_LIBC_ISOC90)
#  define LZO_INFO_LIBC         "isoc90"
#elif (LZO_LIBC_ISOC99)
#  define LZO_INFO_LIBC         "isoc99"
#elif (LZO_CC_ARMCC_ARMCC) && defined(__ARMCLIB_VERSION)
#  define LZO_LIBC_ISOC90       1
#  define LZO_INFO_LIBC         "isoc90"
#elif defined(__dietlibc__)
#  define LZO_LIBC_DIETLIBC     1
#  define LZO_INFO_LIBC         "dietlibc"
#elif defined(_NEWLIB_VERSION)
#  define LZO_LIBC_NEWLIB       1
#  define LZO_INFO_LIBC         "newlib"
#elif defined(__UCLIBC__) && defined(__UCLIBC_MAJOR__) && defined(__UCLIBC_MINOR__)
#  if defined(__UCLIBC_SUBLEVEL__)
#    define LZO_LIBC_UCLIBC     (__UCLIBC_MAJOR__ * 0x10000L + (__UCLIBC_MINOR__-0) * 0x100 + (__UCLIBC_SUBLEVEL__-0))
#  else
#    define LZO_LIBC_UCLIBC     0x00090bL
#  endif
#  define LZO_INFO_LIBC         "uc" "libc"
#elif defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#  define LZO_LIBC_GLIBC        (__GLIBC__ * 0x10000L + (__GLIBC_MINOR__-0) * 0x100)
#  define LZO_INFO_LIBC         "glibc"
#elif (LZO_CC_MWERKS) && defined(__MSL__)
#  define LZO_LIBC_MSL          __MSL__
#  define LZO_INFO_LIBC         "msl"
#elif 1 && defined(__IAR_SYSTEMS_ICC__)
#  define LZO_LIBC_ISOC90       1
#  define LZO_INFO_LIBC         "isoc90"
#else
#  define LZO_LIBC_DEFAULT      1
#  define LZO_INFO_LIBC         "default"
#endif
#endif
#if (LZO_ARCH_I386 && (LZO_OS_DOS32 || LZO_OS_WIN32) && (LZO_CC_DMC || LZO_CC_INTELC || LZO_CC_MSC || LZO_CC_PELLESC))
#  define LZO_ASM_SYNTAX_MSC 1
#elif (LZO_OS_WIN64 && (LZO_CC_DMC || LZO_CC_INTELC || LZO_CC_MSC || LZO_CC_PELLESC))
#elif (LZO_ARCH_I386 && LZO_CC_GNUC && (LZO_CC_GNUC == 0x011f00ul))
#elif (LZO_ARCH_I386 && (LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_INTELC || LZO_CC_PATHSCALE))
#  define LZO_ASM_SYNTAX_GNUC 1
#elif (LZO_ARCH_AMD64 && (LZO_CC_CLANG || LZO_CC_GNUC || LZO_CC_INTELC || LZO_CC_PATHSCALE))
#  define LZO_ASM_SYNTAX_GNUC 1
#elif (LZO_CC_GNUC)
#  define LZO_ASM_SYNTAX_GNUC 1
#endif
#if (LZO_ASM_SYNTAX_GNUC)
#if (LZO_ARCH_I386 && LZO_CC_GNUC && (LZO_CC_GNUC < 0x020000ul))
#  define __LZO_ASM_CLOBBER                     "ax"
#  define __LZO_ASM_CLOBBER_LIST_CC             /*empty*/
#  define __LZO_ASM_CLOBBER_LIST_CC_MEMORY      /*empty*/
#  define __LZO_ASM_CLOBBER_LIST_EMPTY          /*empty*/
#elif (LZO_CC_INTELC && (__INTEL_COMPILER < 1000))
#  define __LZO_ASM_CLOBBER                     "memory"
#  define __LZO_ASM_CLOBBER_LIST_CC             /*empty*/
#  define __LZO_ASM_CLOBBER_LIST_CC_MEMORY      : "memory"
#  define __LZO_ASM_CLOBBER_LIST_EMPTY          /*empty*/
#else
#  define __LZO_ASM_CLOBBER                     "cc", "memory"
#  define __LZO_ASM_CLOBBER_LIST_CC             : "cc"
#  define __LZO_ASM_CLOBBER_LIST_CC_MEMORY      : "cc", "memory"
#  define __LZO_ASM_CLOBBER_LIST_EMPTY          /*empty*/
#endif
#endif
#if (LZO_ARCH_ALPHA)
#  define LZO_OPT_AVOID_UINT_INDEX          1
#elif (LZO_ARCH_AMD64)
#  define LZO_OPT_AVOID_INT_INDEX           1
#  define LZO_OPT_AVOID_UINT_INDEX          1
#  ifndef LZO_OPT_UNALIGNED16
#  define LZO_OPT_UNALIGNED16               1
#  endif
#  ifndef LZO_OPT_UNALIGNED32
#  define LZO_OPT_UNALIGNED32               1
#  endif
#  ifndef LZO_OPT_UNALIGNED64
#  define LZO_OPT_UNALIGNED64               1
#  endif
#elif (LZO_ARCH_ARM)
#  if defined(__ARM_FEATURE_UNALIGNED)
#   if ((__ARM_FEATURE_UNALIGNED)+0)
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#   endif
#  elif 1 && (LZO_ARCH_ARM_THUMB2)
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#  elif 1 && defined(__TARGET_ARCH_ARM) && ((__TARGET_ARCH_ARM)+0 >= 7)
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#  elif 1 && defined(__TARGET_ARCH_ARM) && ((__TARGET_ARCH_ARM)+0 >= 6) && (defined(__TARGET_PROFILE_A) || defined(__TARGET_PROFILE_R))
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#  endif
#elif (LZO_ARCH_ARM64)
#  ifndef LZO_OPT_UNALIGNED16
#  define LZO_OPT_UNALIGNED16               1
#  endif
#  ifndef LZO_OPT_UNALIGNED32
#  define LZO_OPT_UNALIGNED32               1
#  endif
#  ifndef LZO_OPT_UNALIGNED64
#  define LZO_OPT_UNALIGNED64               1
#  endif
#elif (LZO_ARCH_CRIS)
#  ifndef LZO_OPT_UNALIGNED16
#  define LZO_OPT_UNALIGNED16               1
#  endif
#  ifndef LZO_OPT_UNALIGNED32
#  define LZO_OPT_UNALIGNED32               1
#  endif
#elif (LZO_ARCH_I386)
#  ifndef LZO_OPT_UNALIGNED16
#  define LZO_OPT_UNALIGNED16               1
#  endif
#  ifndef LZO_OPT_UNALIGNED32
#  define LZO_OPT_UNALIGNED32               1
#  endif
#elif (LZO_ARCH_IA64)
#  define LZO_OPT_AVOID_INT_INDEX           1
#  define LZO_OPT_AVOID_UINT_INDEX          1
#  define LZO_OPT_PREFER_POSTINC            1
#elif (LZO_ARCH_M68K)
#  define LZO_OPT_PREFER_POSTINC            1
#  define LZO_OPT_PREFER_PREDEC             1
#  if defined(__mc68020__) && !defined(__mcoldfire__)
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#  endif
#elif (LZO_ARCH_MIPS)
#  define LZO_OPT_AVOID_UINT_INDEX          1
#elif (LZO_ARCH_POWERPC)
#  define LZO_OPT_PREFER_PREINC             1
#  define LZO_OPT_PREFER_PREDEC             1
#  if (LZO_ABI_BIG_ENDIAN)
#    ifndef LZO_OPT_UNALIGNED16
#    define LZO_OPT_UNALIGNED16             1
#    endif
#    ifndef LZO_OPT_UNALIGNED32
#    define LZO_OPT_UNALIGNED32             1
#    endif
#    if (LZO_WORDSIZE == 8)
#      ifndef LZO_OPT_UNALIGNED64
#      define LZO_OPT_UNALIGNED64           1
#      endif
#    endif
#  endif
#elif (LZO_ARCH_S390)
#  ifndef LZO_OPT_UNALIGNED16
#  define LZO_OPT_UNALIGNED16               1
#  endif
#  ifndef LZO_OPT_UNALIGNED32
#  define LZO_OPT_UNALIGNED32               1
#  endif
#  if (LZO_WORDSIZE == 8)
#    ifndef LZO_OPT_UNALIGNED64
#    define LZO_OPT_UNALIGNED64             1
#    endif
#  endif
#elif (LZO_ARCH_SH)
#  define LZO_OPT_PREFER_POSTINC            1
#  define LZO_OPT_PREFER_PREDEC             1
#endif
#ifndef LZO_CFG_NO_INLINE_ASM
#if (LZO_ABI_NEUTRAL_ENDIAN) || (LZO_ARCH_GENERIC)
#  define LZO_CFG_NO_INLINE_ASM 1
#elif (LZO_CC_LLVM)
#  define LZO_CFG_NO_INLINE_ASM 1
#endif
#endif
#if (LZO_CFG_NO_INLINE_ASM)
#  undef LZO_ASM_SYNTAX_MSC
#  undef LZO_ASM_SYNTAX_GNUC
#  undef __LZO_ASM_CLOBBER
#  undef __LZO_ASM_CLOBBER_LIST_CC
#  undef __LZO_ASM_CLOBBER_LIST_CC_MEMORY
#  undef __LZO_ASM_CLOBBER_LIST_EMPTY
#endif
#ifndef LZO_CFG_NO_UNALIGNED
#if (LZO_ABI_NEUTRAL_ENDIAN) || (LZO_ARCH_GENERIC)
#  define LZO_CFG_NO_UNALIGNED 1
#endif
#endif
#if (LZO_CFG_NO_UNALIGNED)
#  undef LZO_OPT_UNALIGNED16
#  undef LZO_OPT_UNALIGNED32
#  undef LZO_OPT_UNALIGNED64
#endif
#if defined(__LZO_INFOSTR_MM)
#elif (LZO_MM_FLAT) && (defined(__LZO_INFOSTR_PM) || defined(LZO_INFO_ABI_PM))
#  define __LZO_INFOSTR_MM          ""
#elif defined(LZO_INFO_MM)
#  define __LZO_INFOSTR_MM          "." LZO_INFO_MM
#else
#  define __LZO_INFOSTR_MM          ""
#endif
#if defined(__LZO_INFOSTR_PM)
#elif defined(LZO_INFO_ABI_PM)
#  define __LZO_INFOSTR_PM          "." LZO_INFO_ABI_PM
#else
#  define __LZO_INFOSTR_PM          ""
#endif
#if defined(__LZO_INFOSTR_ENDIAN)
#elif defined(LZO_INFO_ABI_ENDIAN)
#  define __LZO_INFOSTR_ENDIAN      "." LZO_INFO_ABI_ENDIAN
#else
#  define __LZO_INFOSTR_ENDIAN      ""
#endif
#if defined(__LZO_INFOSTR_OSNAME)
#elif defined(LZO_INFO_OS_CONSOLE)
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS "." LZO_INFO_OS_CONSOLE
#elif defined(LZO_INFO_OS_POSIX)
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS "." LZO_INFO_OS_POSIX
#else
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS
#endif
#if defined(__LZO_INFOSTR_LIBC)
#elif defined(LZO_INFO_LIBC)
#  define __LZO_INFOSTR_LIBC        "." LZO_INFO_LIBC
#else
#  define __LZO_INFOSTR_LIBC        ""
#endif
#if defined(__LZO_INFOSTR_CCVER)
#elif defined(LZO_INFO_CCVER)
#  define __LZO_INFOSTR_CCVER       " " LZO_INFO_CCVER
#else
#  define __LZO_INFOSTR_CCVER       ""
#endif
#define LZO_INFO_STRING \
    LZO_INFO_ARCH __LZO_INFOSTR_MM __LZO_INFOSTR_PM __LZO_INFOSTR_ENDIAN \
    " " __LZO_INFOSTR_OSNAME __LZO_INFOSTR_LIBC " " LZO_INFO_CC __LZO_INFOSTR_CCVER
#if !(LZO_CFG_SKIP_LZO_TYPES)
#if (!(LZO_SIZEOF_SHORT+0 > 0 && LZO_SIZEOF_INT+0 > 0 && LZO_SIZEOF_LONG+0 > 0))
#  error "missing defines for sizes"
#endif
#if (!(LZO_SIZEOF_PTRDIFF_T+0 > 0 && LZO_SIZEOF_SIZE_T+0 > 0 && LZO_SIZEOF_VOID_P+0 > 0))
#  error "missing defines for sizes"
#endif
#define LZO_TYPEOF_CHAR             1u
#define LZO_TYPEOF_SHORT            2u
#define LZO_TYPEOF_INT              3u
#define LZO_TYPEOF_LONG             4u
#define LZO_TYPEOF_LONG_LONG        5u
#define LZO_TYPEOF___INT8           17u
#define LZO_TYPEOF___INT16          18u
#define LZO_TYPEOF___INT32          19u
#define LZO_TYPEOF___INT64          20u
#define LZO_TYPEOF___INT128         21u
#define LZO_TYPEOF___INT256         22u
#define LZO_TYPEOF___MODE_QI        33u
#define LZO_TYPEOF___MODE_HI        34u
#define LZO_TYPEOF___MODE_SI        35u
#define LZO_TYPEOF___MODE_DI        36u
#define LZO_TYPEOF___MODE_TI        37u
#define LZO_TYPEOF_CHAR_P           129u
#if !defined(lzo_llong_t)
#if (LZO_SIZEOF_LONG_LONG+0 > 0)
__lzo_gnuc_extension__ typedef long long lzo_llong_t__;
__lzo_gnuc_extension__ typedef unsigned long long lzo_ullong_t__;
#  define lzo_llong_t               lzo_llong_t__
#  define lzo_ullong_t              lzo_ullong_t__
#endif
#endif
#if !defined(lzo_int16e_t)
#if (LZO_SIZEOF_LONG == 2)
#  define lzo_int16e_t              long
#  define lzo_uint16e_t             unsigned long
#  define LZO_TYPEOF_LZO_INT16E_T   LZO_TYPEOF_LONG
#elif (LZO_SIZEOF_INT == 2)
#  define lzo_int16e_t              int
#  define lzo_uint16e_t             unsigned int
#  define LZO_TYPEOF_LZO_INT16E_T   LZO_TYPEOF_INT
#elif (LZO_SIZEOF_SHORT == 2)
#  define lzo_int16e_t              short int
#  define lzo_uint16e_t             unsigned short int
#  define LZO_TYPEOF_LZO_INT16E_T   LZO_TYPEOF_SHORT
#elif 1 && !(LZO_CFG_TYPE_NO_MODE_HI) && (LZO_CC_CLANG || (LZO_CC_GNUC >= 0x025f00ul) || LZO_CC_LLVM)
   typedef int lzo_int16e_hi_t__ __attribute__((__mode__(__HI__)));
   typedef unsigned int lzo_uint16e_hi_t__ __attribute__((__mode__(__HI__)));
#  define lzo_int16e_t              lzo_int16e_hi_t__
#  define lzo_uint16e_t             lzo_uint16e_hi_t__
#  define LZO_TYPEOF_LZO_INT16E_T   LZO_TYPEOF___MODE_HI
#elif (LZO_SIZEOF___INT16 == 2)
#  define lzo_int16e_t              __int16
#  define lzo_uint16e_t             unsigned __int16
#  define LZO_TYPEOF_LZO_INT16E_T   LZO_TYPEOF___INT16
#else
#endif
#endif
#if defined(lzo_int16e_t)
#  define LZO_SIZEOF_LZO_INT16E_T   2
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int16e_t) == 2)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int16e_t) == LZO_SIZEOF_LZO_INT16E_T)
#endif
#if !defined(lzo_int32e_t)
#if (LZO_SIZEOF_LONG == 4)
#  define lzo_int32e_t              long int
#  define lzo_uint32e_t             unsigned long int
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF_LONG
#elif (LZO_SIZEOF_INT == 4)
#  define lzo_int32e_t              int
#  define lzo_uint32e_t             unsigned int
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF_INT
#elif (LZO_SIZEOF_SHORT == 4)
#  define lzo_int32e_t              short int
#  define lzo_uint32e_t             unsigned short int
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF_SHORT
#elif (LZO_SIZEOF_LONG_LONG == 4)
#  define lzo_int32e_t              lzo_llong_t
#  define lzo_uint32e_t             lzo_ullong_t
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF_LONG_LONG
#elif 1 && !(LZO_CFG_TYPE_NO_MODE_SI) && (LZO_CC_CLANG || (LZO_CC_GNUC >= 0x025f00ul) || LZO_CC_LLVM) && (__INT_MAX__+0 > 2147483647L)
   typedef int lzo_int32e_si_t__ __attribute__((__mode__(__SI__)));
   typedef unsigned int lzo_uint32e_si_t__ __attribute__((__mode__(__SI__)));
#  define lzo_int32e_t              lzo_int32e_si_t__
#  define lzo_uint32e_t             lzo_uint32e_si_t__
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF___MODE_SI
#elif 1 && !(LZO_CFG_TYPE_NO_MODE_SI) && (LZO_CC_GNUC >= 0x025f00ul) && defined(__AVR__) && (__LONG_MAX__+0 == 32767L)
   typedef int lzo_int32e_si_t__ __attribute__((__mode__(__SI__)));
   typedef unsigned int lzo_uint32e_si_t__ __attribute__((__mode__(__SI__)));
#  define lzo_int32e_t              lzo_int32e_si_t__
#  define lzo_uint32e_t             lzo_uint32e_si_t__
#  define LZO_INT32_C(c)            (c##LL)
#  define LZO_UINT32_C(c)           (c##ULL)
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF___MODE_SI
#elif (LZO_SIZEOF___INT32 == 4)
#  define lzo_int32e_t              __int32
#  define lzo_uint32e_t             unsigned __int32
#  define LZO_TYPEOF_LZO_INT32E_T   LZO_TYPEOF___INT32
#else
#endif
#endif
#if defined(lzo_int32e_t)
#  define LZO_SIZEOF_LZO_INT32E_T   4
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32e_t) == 4)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32e_t) == LZO_SIZEOF_LZO_INT32E_T)
#endif
#if !defined(lzo_int64e_t)
#if (LZO_SIZEOF___INT64 == 8)
#  if (LZO_CC_BORLANDC) && !(LZO_CFG_TYPE_PREFER___INT64)
#    define LZO_CFG_TYPE_PREFER___INT64 1
#  endif
#endif
#if (LZO_SIZEOF_INT == 8) && (LZO_SIZEOF_INT < LZO_SIZEOF_LONG)
#  define lzo_int64e_t              int
#  define lzo_uint64e_t             unsigned int
#  define LZO_TYPEOF_LZO_INT64E_T   LZO_TYPEOF_INT
#elif (LZO_SIZEOF_LONG == 8)
#  define lzo_int64e_t              long int
#  define lzo_uint64e_t             unsigned long int
#  define LZO_TYPEOF_LZO_INT64E_T   LZO_TYPEOF_LONG
#elif (LZO_SIZEOF_LONG_LONG == 8) && !(LZO_CFG_TYPE_PREFER___INT64)
#  define lzo_int64e_t              lzo_llong_t
#  define lzo_uint64e_t             lzo_ullong_t
#  define LZO_TYPEOF_LZO_INT64E_T   LZO_TYPEOF_LONG_LONG
#  if (LZO_CC_BORLANDC)
#    define LZO_INT64_C(c)          ((c) + 0ll)
#    define LZO_UINT64_C(c)         ((c) + 0ull)
#  elif 0
#    define LZO_INT64_C(c)          (__lzo_gnuc_extension__ (c##LL))
#    define LZO_UINT64_C(c)         (__lzo_gnuc_extension__ (c##ULL))
#  else
#    define LZO_INT64_C(c)          (c##LL)
#    define LZO_UINT64_C(c)         (c##ULL)
#  endif
#elif (LZO_SIZEOF___INT64 == 8)
#  define lzo_int64e_t              __int64
#  define lzo_uint64e_t             unsigned __int64
#  define LZO_TYPEOF_LZO_INT64E_T   LZO_TYPEOF___INT64
#  if (LZO_CC_BORLANDC)
#    define LZO_INT64_C(c)          ((c) + 0i64)
#    define LZO_UINT64_C(c)         ((c) + 0ui64)
#  else
#    define LZO_INT64_C(c)          (c##i64)
#    define LZO_UINT64_C(c)         (c##ui64)
#  endif
#else
#endif
#endif
#if defined(lzo_int64e_t)
#  define LZO_SIZEOF_LZO_INT64E_T   8
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64e_t) == 8)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64e_t) == LZO_SIZEOF_LZO_INT64E_T)
#endif
#if !defined(lzo_int32l_t)
#if defined(lzo_int32e_t)
#  define lzo_int32l_t              lzo_int32e_t
#  define lzo_uint32l_t             lzo_uint32e_t
#  define LZO_SIZEOF_LZO_INT32L_T   LZO_SIZEOF_LZO_INT32E_T
#  define LZO_TYPEOF_LZO_INT32L_T   LZO_TYPEOF_LZO_INT32E_T
#elif (LZO_SIZEOF_INT >= 4) && (LZO_SIZEOF_INT < LZO_SIZEOF_LONG)
#  define lzo_int32l_t              int
#  define lzo_uint32l_t             unsigned int
#  define LZO_SIZEOF_LZO_INT32L_T   LZO_SIZEOF_INT
#  define LZO_TYPEOF_LZO_INT32L_T   LZO_SIZEOF_INT
#elif (LZO_SIZEOF_LONG >= 4)
#  define lzo_int32l_t              long int
#  define lzo_uint32l_t             unsigned long int
#  define LZO_SIZEOF_LZO_INT32L_T   LZO_SIZEOF_LONG
#  define LZO_TYPEOF_LZO_INT32L_T   LZO_SIZEOF_LONG
#else
#  error "lzo_int32l_t"
#endif
#endif
#if 1
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32l_t) >= 4)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32l_t) == LZO_SIZEOF_LZO_INT32L_T)
#endif
#if !defined(lzo_int64l_t)
#if defined(lzo_int64e_t)
#  define lzo_int64l_t              lzo_int64e_t
#  define lzo_uint64l_t             lzo_uint64e_t
#  define LZO_SIZEOF_LZO_INT64L_T   LZO_SIZEOF_LZO_INT64E_T
#  define LZO_TYPEOF_LZO_INT64L_T   LZO_TYPEOF_LZO_INT64E_T
#else
#endif
#endif
#if defined(lzo_int64l_t)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64l_t) >= 8)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64l_t) == LZO_SIZEOF_LZO_INT64L_T)
#endif
#if !defined(lzo_int32f_t)
#if (LZO_SIZEOF_SIZE_T >= 8)
#  define lzo_int32f_t              lzo_int64l_t
#  define lzo_uint32f_t             lzo_uint64l_t
#  define LZO_SIZEOF_LZO_INT32F_T   LZO_SIZEOF_LZO_INT64L_T
#  define LZO_TYPEOF_LZO_INT32F_T   LZO_TYPEOF_LZO_INT64L_T
#else
#  define lzo_int32f_t              lzo_int32l_t
#  define lzo_uint32f_t             lzo_uint32l_t
#  define LZO_SIZEOF_LZO_INT32F_T   LZO_SIZEOF_LZO_INT32L_T
#  define LZO_TYPEOF_LZO_INT32F_T   LZO_TYPEOF_LZO_INT32L_T
#endif
#endif
#if 1
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32f_t) >= 4)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32f_t) == LZO_SIZEOF_LZO_INT32F_T)
#endif
#if !defined(lzo_int64f_t)
#if defined(lzo_int64l_t)
#  define lzo_int64f_t              lzo_int64l_t
#  define lzo_uint64f_t             lzo_uint64l_t
#  define LZO_SIZEOF_LZO_INT64F_T   LZO_SIZEOF_LZO_INT64L_T
#  define LZO_TYPEOF_LZO_INT64F_T   LZO_TYPEOF_LZO_INT64L_T
#else
#endif
#endif
#if defined(lzo_int64f_t)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64f_t) >= 8)
   LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64f_t) == LZO_SIZEOF_LZO_INT64F_T)
#endif
#if !defined(lzo_intptr_t)
#if 1 && (LZO_OS_OS400 && (LZO_SIZEOF_VOID_P == 16))
#  define __LZO_INTPTR_T_IS_POINTER 1
   typedef char *                   lzo_intptr_t;
   typedef char *                   lzo_uintptr_t;
#  define lzo_intptr_t              lzo_intptr_t
#  define lzo_uintptr_t             lzo_uintptr_t
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_VOID_P
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_CHAR_P
#elif (LZO_CC_MSC && (_MSC_VER >= 1300) && (LZO_SIZEOF_VOID_P == 4) && (LZO_SIZEOF_INT == 4))
   typedef __w64 int                lzo_intptr_t;
   typedef __w64 unsigned int       lzo_uintptr_t;
#  define lzo_intptr_t              lzo_intptr_t
#  define lzo_uintptr_t             lzo_uintptr_t
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_INT
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_INT
#elif (LZO_SIZEOF_SHORT == LZO_SIZEOF_VOID_P) && (LZO_SIZEOF_INT > LZO_SIZEOF_VOID_P)
#  define lzo_intptr_t              short
#  define lzo_uintptr_t             unsigned short
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_SHORT
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_SHORT
#elif (LZO_SIZEOF_INT >= LZO_SIZEOF_VOID_P) && (LZO_SIZEOF_INT < LZO_SIZEOF_LONG)
#  define lzo_intptr_t              int
#  define lzo_uintptr_t             unsigned int
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_INT
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_INT
#elif (LZO_SIZEOF_LONG >= LZO_SIZEOF_VOID_P)
#  define lzo_intptr_t              long
#  define lzo_uintptr_t             unsigned long
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_LONG
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_LONG
#elif (LZO_SIZEOF_LZO_INT64L_T >= LZO_SIZEOF_VOID_P)
#  define lzo_intptr_t              lzo_int64l_t
#  define lzo_uintptr_t             lzo_uint64l_t
#  define LZO_SIZEOF_LZO_INTPTR_T   LZO_SIZEOF_LZO_INT64L_T
#  define LZO_TYPEOF_LZO_INTPTR_T   LZO_TYPEOF_LZO_INT64L_T
#else
#  error "lzo_intptr_t"
#endif
#endif
#if 1
    LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_intptr_t) >= sizeof(void *))
    LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_intptr_t) == sizeof(lzo_uintptr_t))
#endif
#if !defined(lzo_word_t)
#if defined(LZO_WORDSIZE) && (LZO_WORDSIZE+0 > 0)
#if (LZO_WORDSIZE == LZO_SIZEOF_LZO_INTPTR_T) && !(__LZO_INTPTR_T_IS_POINTER)
#  define lzo_word_t                lzo_uintptr_t
#  define lzo_sword_t               lzo_intptr_t
#  define LZO_SIZEOF_LZO_WORD_T     LZO_SIZEOF_LZO_INTPTR_T
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF_LZO_INTPTR_T
#elif (LZO_WORDSIZE == LZO_SIZEOF_LONG)
#  define lzo_word_t                unsigned long
#  define lzo_sword_t               long
#  define LZO_SIZEOF_LZO_WORD_T     LZO_SIZEOF_LONG
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF_LONG
#elif (LZO_WORDSIZE == LZO_SIZEOF_INT)
#  define lzo_word_t                unsigned int
#  define lzo_sword_t               int
#  define LZO_SIZEOF_LZO_WORD_T     LZO_SIZEOF_INT
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF_INT
#elif (LZO_WORDSIZE == LZO_SIZEOF_SHORT)
#  define lzo_word_t                unsigned short
#  define lzo_sword_t               short
#  define LZO_SIZEOF_LZO_WORD_T     LZO_SIZEOF_SHORT
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF_SHORT
#elif (LZO_WORDSIZE == 1)
#  define lzo_word_t                unsigned char
#  define lzo_sword_t               signed char
#  define LZO_SIZEOF_LZO_WORD_T     1
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF_CHAR
#elif (LZO_WORDSIZE == LZO_SIZEOF_LZO_INT64L_T)
#  define lzo_word_t                lzo_uint64l_t
#  define lzo_sword_t               lzo_int64l_t
#  define LZO_SIZEOF_LZO_WORD_T     LZO_SIZEOF_LZO_INT64L_T
#  define LZO_TYPEOF_LZO_WORD_T     LZO_SIZEOF_LZO_INT64L_T
#elif (LZO_ARCH_SPU) && (LZO_CC_GNUC)
#if 0
   typedef unsigned lzo_word_t  __attribute__((__mode__(__V16QI__)));
   typedef int      lzo_sword_t __attribute__((__mode__(__V16QI__)));
#  define lzo_word_t                lzo_word_t
#  define lzo_sword_t               lzo_sword_t
#  define LZO_SIZEOF_LZO_WORD_T     16
#  define LZO_TYPEOF_LZO_WORD_T     LZO_TYPEOF___MODE_V16QI
#endif
#else
#  error "lzo_word_t"
#endif
#endif
#endif
#if 1 && defined(lzo_word_t)
    LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_word_t)  == LZO_WORDSIZE)
    LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_sword_t) == LZO_WORDSIZE)
#endif
#if 1
#define lzo_int8_t                  signed char
#define lzo_uint8_t                 unsigned char
#define LZO_SIZEOF_LZO_INT8_T       1
#define LZO_TYPEOF_LZO_INT8_T       LZO_TYPEOF_CHAR
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int8_t) == 1)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int8_t) == sizeof(lzo_uint8_t))
#endif
#if defined(lzo_int16e_t)
#define lzo_int16_t                 lzo_int16e_t
#define lzo_uint16_t                lzo_uint16e_t
#define LZO_SIZEOF_LZO_INT16_T      LZO_SIZEOF_LZO_INT16E_T
#define LZO_TYPEOF_LZO_INT16_T      LZO_TYPEOF_LZO_INT16E_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int16_t) == 2)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int16_t) == sizeof(lzo_uint16_t))
#endif
#if defined(lzo_int32e_t)
#define lzo_int32_t                 lzo_int32e_t
#define lzo_uint32_t                lzo_uint32e_t
#define LZO_SIZEOF_LZO_INT32_T      LZO_SIZEOF_LZO_INT32E_T
#define LZO_TYPEOF_LZO_INT32_T      LZO_TYPEOF_LZO_INT32E_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32_t) == 4)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32_t) == sizeof(lzo_uint32_t))
#endif
#if defined(lzo_int64e_t)
#define lzo_int64_t                 lzo_int64e_t
#define lzo_uint64_t                lzo_uint64e_t
#define LZO_SIZEOF_LZO_INT64_T      LZO_SIZEOF_LZO_INT64E_T
#define LZO_TYPEOF_LZO_INT64_T      LZO_TYPEOF_LZO_INT64E_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64_t) == 8)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64_t) == sizeof(lzo_uint64_t))
#endif
#if 1
#define lzo_int_least32_t           lzo_int32l_t
#define lzo_uint_least32_t          lzo_uint32l_t
#define LZO_SIZEOF_LZO_INT_LEAST32_T LZO_SIZEOF_LZO_INT32L_T
#define LZO_TYPEOF_LZO_INT_LEAST32_T LZO_TYPEOF_LZO_INT32L_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_least32_t) >= 4)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_least32_t) == sizeof(lzo_uint_least32_t))
#endif
#if defined(lzo_int64l_t)
#define lzo_int_least64_t           lzo_int64l_t
#define lzo_uint_least64_t          lzo_uint64l_t
#define LZO_SIZEOF_LZO_INT_LEAST64_T LZO_SIZEOF_LZO_INT64L_T
#define LZO_TYPEOF_LZO_INT_LEAST64_T LZO_TYPEOF_LZO_INT64L_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_least64_t) >= 8)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_least64_t) == sizeof(lzo_uint_least64_t))
#endif
#if 1
#define lzo_int_fast32_t           lzo_int32f_t
#define lzo_uint_fast32_t          lzo_uint32f_t
#define LZO_SIZEOF_LZO_INT_FAST32_T LZO_SIZEOF_LZO_INT32F_T
#define LZO_TYPEOF_LZO_INT_FAST32_T LZO_TYPEOF_LZO_INT32F_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_fast32_t) >= 4)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_fast32_t) == sizeof(lzo_uint_fast32_t))
#endif
#if defined(lzo_int64f_t)
#define lzo_int_fast64_t           lzo_int64f_t
#define lzo_uint_fast64_t          lzo_uint64f_t
#define LZO_SIZEOF_LZO_INT_FAST64_T LZO_SIZEOF_LZO_INT64F_T
#define LZO_TYPEOF_LZO_INT_FAST64_T LZO_TYPEOF_LZO_INT64F_T
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_fast64_t) >= 8)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int_fast64_t) == sizeof(lzo_uint_fast64_t))
#endif
#if !defined(LZO_INT16_C)
#  if (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_INT >= 2)
#    define LZO_INT16_C(c)          ((c) + 0)
#    define LZO_UINT16_C(c)         ((c) + 0U)
#  elif (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_LONG >= 2)
#    define LZO_INT16_C(c)          ((c) + 0L)
#    define LZO_UINT16_C(c)         ((c) + 0UL)
#  elif (LZO_SIZEOF_INT >= 2)
#    define LZO_INT16_C(c)          (c)
#    define LZO_UINT16_C(c)         (c##U)
#  elif (LZO_SIZEOF_LONG >= 2)
#    define LZO_INT16_C(c)          (c##L)
#    define LZO_UINT16_C(c)         (c##UL)
#  else
#    error "LZO_INT16_C"
#  endif
#endif
#if !defined(LZO_INT32_C)
#  if (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_INT >= 4)
#    define LZO_INT32_C(c)          ((c) + 0)
#    define LZO_UINT32_C(c)         ((c) + 0U)
#  elif (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_LONG >= 4)
#    define LZO_INT32_C(c)          ((c) + 0L)
#    define LZO_UINT32_C(c)         ((c) + 0UL)
#  elif (LZO_SIZEOF_INT >= 4)
#    define LZO_INT32_C(c)          (c)
#    define LZO_UINT32_C(c)         (c##U)
#  elif (LZO_SIZEOF_LONG >= 4)
#    define LZO_INT32_C(c)          (c##L)
#    define LZO_UINT32_C(c)         (c##UL)
#  elif (LZO_SIZEOF_LONG_LONG >= 4)
#    define LZO_INT32_C(c)          (c##LL)
#    define LZO_UINT32_C(c)         (c##ULL)
#  else
#    error "LZO_INT32_C"
#  endif
#endif
#if !defined(LZO_INT64_C) && defined(lzo_int64l_t)
#  if (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_INT >= 8)
#    define LZO_INT64_C(c)          ((c) + 0)
#    define LZO_UINT64_C(c)         ((c) + 0U)
#  elif (LZO_BROKEN_INTEGRAL_CONSTANTS) && (LZO_SIZEOF_LONG >= 8)
#    define LZO_INT64_C(c)          ((c) + 0L)
#    define LZO_UINT64_C(c)         ((c) + 0UL)
#  elif (LZO_SIZEOF_INT >= 8)
#    define LZO_INT64_C(c)          (c)
#    define LZO_UINT64_C(c)         (c##U)
#  elif (LZO_SIZEOF_LONG >= 8)
#    define LZO_INT64_C(c)          (c##L)
#    define LZO_UINT64_C(c)         (c##UL)
#  else
#    error "LZO_INT64_C"
#  endif
#endif
#endif

#endif

#endif

#undef LZO_HAVE_CONFIG_H
#include "minilzo.h"

#if !defined(MINILZO_VERSION) || (MINILZO_VERSION != 0x2090)
#  error "version mismatch in miniLZO source files"
#endif

#ifdef MINILZO_HAVE_CONFIG_H
#  define LZO_HAVE_CONFIG_H 1
#endif

#ifndef __LZO_CONF_H
#define __LZO_CONF_H 1

#if !defined(__LZO_IN_MINILZO)
#if defined(LZO_CFG_FREESTANDING) && (LZO_CFG_FREESTANDING)
#  define LZO_LIBC_FREESTANDING 1
#  define LZO_OS_FREESTANDING 1
#endif
#if defined(LZO_CFG_EXTRA_CONFIG_HEADER)
#  include LZO_CFG_EXTRA_CONFIG_HEADER
#endif
#if defined(__LZOCONF_H) || defined(__LZOCONF_H_INCLUDED)
#  error "include this file first"
#endif
#if defined(LZO_CFG_BUILD_DLL) && (LZO_CFG_BUILD_DLL+0) && !defined(__LZO_EXPORT1) && !defined(__LZO_EXPORT2) && 0
#ifndef __LZODEFS_H_INCLUDED
#if defined(LZO_HAVE_CONFIG_H)
#  include <config.h>
#endif
#include <limits.h>
#include <stddef.h>
#include <lzo/lzodefs.h>
#endif
#endif
#include <lzo/lzoconf.h>
#if defined(LZO_CFG_EXTRA_CONFIG_HEADER2)
#  include LZO_CFG_EXTRA_CONFIG_HEADER2
#endif
#endif

#if !defined(__LZOCONF_H_INCLUDED) || (LZO_VERSION+0 != 0x2090)
#  error "version mismatch"
#endif

#if (LZO_CC_MSC && (_MSC_VER >= 1000 && _MSC_VER < 1100))
#  pragma warning(disable: 4702)
#endif
#if (LZO_CC_MSC && (_MSC_VER >= 1000))
#  pragma warning(disable: 4127 4701)
#  pragma warning(disable: 4514 4710 4711)
#endif
#if (LZO_CC_MSC && (_MSC_VER >= 1300))
#  pragma warning(disable: 4820)
#endif
#if (LZO_CC_MSC && (_MSC_VER >= 1800))
#  pragma warning(disable: 4746)
#endif
#if (LZO_CC_INTELC && (__INTEL_COMPILER >= 900))
#  pragma warning(disable: 1684)
#endif

#if (LZO_CC_SUNPROC)
#if !defined(__cplusplus)
#  pragma error_messages(off,E_END_OF_LOOP_CODE_NOT_REACHED)
#  pragma error_messages(off,E_LOOP_NOT_ENTERED_AT_TOP)
#  pragma error_messages(off,E_STATEMENT_NOT_REACHED)
#endif
#endif

#if !defined(__LZO_NOEXPORT1)
#  define __LZO_NOEXPORT1       /*empty*/
#endif
#if !defined(__LZO_NOEXPORT2)
#  define __LZO_NOEXPORT2       /*empty*/
#endif

#if 1
#  define LZO_PUBLIC_DECL(r)    LZO_EXTERN(r)
#endif
#if 1
#  define LZO_PUBLIC_IMPL(r)    LZO_PUBLIC(r)
#endif
#if !defined(LZO_LOCAL_DECL)
#  define LZO_LOCAL_DECL(r)     __LZO_EXTERN_C LZO_LOCAL_IMPL(r)
#endif
#if !defined(LZO_LOCAL_IMPL)
#  define LZO_LOCAL_IMPL(r)     __LZO_NOEXPORT1 r __LZO_NOEXPORT2 __LZO_CDECL
#endif
#if 1
#  define LZO_STATIC_DECL(r)    LZO_PRIVATE(r)
#endif
#if 1
#  define LZO_STATIC_IMPL(r)    LZO_PRIVATE(r)
#endif

#if defined(__LZO_IN_MINILZO) || (LZO_CFG_FREESTANDING)
#elif 1
#  include <string.h>
#else
#  define LZO_WANT_ACC_INCD_H 1
#endif
#if defined(LZO_HAVE_CONFIG_H)
#  define LZO_CFG_NO_CONFIG_HEADER 1
#endif

#if 1 && !defined(LZO_CFG_FREESTANDING)
#if 1 && !defined(HAVE_STRING_H)
#define HAVE_STRING_H 1
#endif
#if 1 && !defined(HAVE_MEMCMP)
#define HAVE_MEMCMP 1
#endif
#if 1 && !defined(HAVE_MEMCPY)
#define HAVE_MEMCPY 1
#endif
#if 1 && !defined(HAVE_MEMMOVE)
#define HAVE_MEMMOVE 1
#endif
#if 1 && !defined(HAVE_MEMSET)
#define HAVE_MEMSET 1
#endif
#endif

#if 1 && defined(HAVE_STRING_H)
#include <string.h>
#endif

#if 1 || defined(lzo_int8_t) || defined(lzo_uint8_t)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int8_t)  == 1)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint8_t) == 1)
#endif
#if 1 || defined(lzo_int16_t) || defined(lzo_uint16_t)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int16_t)  == 2)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint16_t) == 2)
#endif
#if 1 || defined(lzo_int32_t) || defined(lzo_uint32_t)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int32_t)  == 4)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint32_t) == 4)
#endif
#if defined(lzo_int64_t) || defined(lzo_uint64_t)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_int64_t)  == 8)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint64_t) == 8)
#endif

#if (LZO_CFG_FREESTANDING)
#  undef HAVE_MEMCMP
#  undef HAVE_MEMCPY
#  undef HAVE_MEMMOVE
#  undef HAVE_MEMSET
#endif

#if !(HAVE_MEMCMP)
#  undef memcmp
#  define memcmp(a,b,c)         lzo_memcmp(a,b,c)
#else
#  undef lzo_memcmp
#  define lzo_memcmp(a,b,c)     memcmp(a,b,c)
#endif
#if !(HAVE_MEMCPY)
#  undef memcpy
#  define memcpy(a,b,c)         lzo_memcpy(a,b,c)
#else
#  undef lzo_memcpy
#  define lzo_memcpy(a,b,c)     memcpy(a,b,c)
#endif
#if !(HAVE_MEMMOVE)
#  undef memmove
#  define memmove(a,b,c)        lzo_memmove(a,b,c)
#else
#  undef lzo_memmove
#  define lzo_memmove(a,b,c)    memmove(a,b,c)
#endif
#if !(HAVE_MEMSET)
#  undef memset
#  define memset(a,b,c)         lzo_memset(a,b,c)
#else
#  undef lzo_memset
#  define lzo_memset(a,b,c)     memset(a,b,c)
#endif

#undef NDEBUG
#if (LZO_CFG_FREESTANDING)
#  undef LZO_DEBUG
#  define NDEBUG 1
#  undef assert
#  define assert(e) ((void)0)
#else
#  if !defined(LZO_DEBUG)
#    define NDEBUG 1
#  endif
#  include <assert.h>
#endif

#if 0 && defined(__BOUNDS_CHECKING_ON)
#  include <unchecked.h>
#else
#  define BOUNDS_CHECKING_OFF_DURING(stmt)      stmt
#  define BOUNDS_CHECKING_OFF_IN_EXPR(expr)     (expr)
#endif

#if (LZO_CFG_PGO)
#  undef __lzo_likely
#  undef __lzo_unlikely
#  define __lzo_likely(e)       (e)
#  define __lzo_unlikely(e)     (e)
#endif

#undef _
#undef __
#undef ___
#undef ____
#undef _p0
#undef _p1
#undef _p2
#undef _p3
#undef _p4
#undef _s0
#undef _s1
#undef _s2
#undef _s3
#undef _s4
#undef _ww

#if 1
#  define LZO_BYTE(x)       ((unsigned char) (x))
#else
#  define LZO_BYTE(x)       ((unsigned char) ((x) & 0xff))
#endif

#define LZO_MAX(a,b)        ((a) >= (b) ? (a) : (b))
#define LZO_MIN(a,b)        ((a) <= (b) ? (a) : (b))
#define LZO_MAX3(a,b,c)     ((a) >= (b) ? LZO_MAX(a,c) : LZO_MAX(b,c))
#define LZO_MIN3(a,b,c)     ((a) <= (b) ? LZO_MIN(a,c) : LZO_MIN(b,c))

#define lzo_sizeof(type)    ((lzo_uint) (sizeof(type)))

#define LZO_HIGH(array)     ((lzo_uint) (sizeof(array)/sizeof(*(array))))

#define LZO_SIZE(bits)      (1u << (bits))
#define LZO_MASK(bits)      (LZO_SIZE(bits) - 1)

#define LZO_USIZE(bits)     ((lzo_uint) 1 << (bits))
#define LZO_UMASK(bits)     (LZO_USIZE(bits) - 1)

#if !defined(DMUL)
#if 0

#  define DMUL(a,b) ((lzo_xint) ((lzo_uint32_t)(a) * (lzo_uint32_t)(b)))
#else
#  define DMUL(a,b) ((lzo_xint) ((a) * (b)))
#endif
#endif

#ifndef __LZO_FUNC_H
#define __LZO_FUNC_H 1

#if !defined(LZO_BITOPS_USE_ASM_BITSCAN) && !defined(LZO_BITOPS_USE_GNUC_BITSCAN) && !defined(LZO_BITOPS_USE_MSC_BITSCAN)
#if 1 && (LZO_ARCH_AMD64) && (LZO_CC_GNUC && (LZO_CC_GNUC < 0x040000ul)) && (LZO_ASM_SYNTAX_GNUC)
#define LZO_BITOPS_USE_ASM_BITSCAN 1
#elif (LZO_CC_CLANG || (LZO_CC_GNUC >= 0x030400ul) || (LZO_CC_INTELC_GNUC && (__INTEL_COMPILER >= 1000)) || (LZO_CC_LLVM && (!defined(__llvm_tools_version__) || (__llvm_tools_version__+0 >= 0x010500ul))))
#define LZO_BITOPS_USE_GNUC_BITSCAN 1
#elif (LZO_OS_WIN32 || LZO_OS_WIN64) && ((LZO_CC_INTELC_MSC && (__INTEL_COMPILER >= 1010)) || (LZO_CC_MSC && (_MSC_VER >= 1400)))
#define LZO_BITOPS_USE_MSC_BITSCAN 1
#if (LZO_CC_MSC) && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
#include <intrin.h>
#endif
#if (LZO_CC_MSC) && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward)
#endif
#if (LZO_CC_MSC) && (LZO_ARCH_AMD64)
#pragma intrinsic(_BitScanReverse64)
#pragma intrinsic(_BitScanForward64)
#endif
#endif
#endif

__lzo_static_forceinline unsigned lzo_bitops_ctlz32_func(lzo_uint32_t v)
{
#if (LZO_BITOPS_USE_MSC_BITSCAN) && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
    unsigned long r; (void) _BitScanReverse(&r, v); return (unsigned) r ^ 31;
#define lzo_bitops_ctlz32(v)    lzo_bitops_ctlz32_func(v)
#elif (LZO_BITOPS_USE_ASM_BITSCAN) && (LZO_ARCH_AMD64 || LZO_ARCH_I386) && (LZO_ASM_SYNTAX_GNUC)
    lzo_uint32_t r;
    __asm__("bsr %1,%0" : "=r" (r) : "rm" (v) __LZO_ASM_CLOBBER_LIST_CC);
    return (unsigned) r ^ 31;
#define lzo_bitops_ctlz32(v)    lzo_bitops_ctlz32_func(v)
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_INT == 4)
    unsigned r; r = (unsigned) __builtin_clz(v); return r;
#define lzo_bitops_ctlz32(v)    ((unsigned) __builtin_clz(v))
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_LONG == 8) && (LZO_WORDSIZE >= 8)
    unsigned r; r = (unsigned) __builtin_clzl(v); return r ^ 32;
#define lzo_bitops_ctlz32(v)    (((unsigned) __builtin_clzl(v)) ^ 32)
#else
    LZO_UNUSED(v); return 0;
#endif
}

#if defined(lzo_uint64_t)
__lzo_static_forceinline unsigned lzo_bitops_ctlz64_func(lzo_uint64_t v)
{
#if (LZO_BITOPS_USE_MSC_BITSCAN) && (LZO_ARCH_AMD64)
    unsigned long r; (void) _BitScanReverse64(&r, v); return (unsigned) r ^ 63;
#define lzo_bitops_ctlz64(v)    lzo_bitops_ctlz64_func(v)
#elif (LZO_BITOPS_USE_ASM_BITSCAN) && (LZO_ARCH_AMD64) && (LZO_ASM_SYNTAX_GNUC)
    lzo_uint64_t r;
    __asm__("bsr %1,%0" : "=r" (r) : "rm" (v) __LZO_ASM_CLOBBER_LIST_CC);
    return (unsigned) r ^ 63;
#define lzo_bitops_ctlz64(v)    lzo_bitops_ctlz64_func(v)
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_LONG == 8) && (LZO_WORDSIZE >= 8)
    unsigned r; r = (unsigned) __builtin_clzl(v); return r;
#define lzo_bitops_ctlz64(v)    ((unsigned) __builtin_clzl(v))
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_LONG_LONG == 8) && (LZO_WORDSIZE >= 8)
    unsigned r; r = (unsigned) __builtin_clzll(v); return r;
#define lzo_bitops_ctlz64(v)    ((unsigned) __builtin_clzll(v))
#else
    LZO_UNUSED(v); return 0;
#endif
}
#endif

__lzo_static_forceinline unsigned lzo_bitops_cttz32_func(lzo_uint32_t v)
{
#if (LZO_BITOPS_USE_MSC_BITSCAN) && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
    unsigned long r; (void) _BitScanForward(&r, v); return (unsigned) r;
#define lzo_bitops_cttz32(v)    lzo_bitops_cttz32_func(v)
#elif (LZO_BITOPS_USE_ASM_BITSCAN) && (LZO_ARCH_AMD64 || LZO_ARCH_I386) && (LZO_ASM_SYNTAX_GNUC)
    lzo_uint32_t r;
    __asm__("bsf %1,%0" : "=r" (r) : "rm" (v) __LZO_ASM_CLOBBER_LIST_CC);
    return (unsigned) r;
#define lzo_bitops_cttz32(v)    lzo_bitops_cttz32_func(v)
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_INT >= 4)
    unsigned r; r = (unsigned) __builtin_ctz(v); return r;
#define lzo_bitops_cttz32(v)    ((unsigned) __builtin_ctz(v))
#else
    LZO_UNUSED(v); return 0;
#endif
}

#if defined(lzo_uint64_t)
__lzo_static_forceinline unsigned lzo_bitops_cttz64_func(lzo_uint64_t v)
{
#if (LZO_BITOPS_USE_MSC_BITSCAN) && (LZO_ARCH_AMD64)
    unsigned long r; (void) _BitScanForward64(&r, v); return (unsigned) r;
#define lzo_bitops_cttz64(v)    lzo_bitops_cttz64_func(v)
#elif (LZO_BITOPS_USE_ASM_BITSCAN) && (LZO_ARCH_AMD64) && (LZO_ASM_SYNTAX_GNUC)
    lzo_uint64_t r;
    __asm__("bsf %1,%0" : "=r" (r) : "rm" (v) __LZO_ASM_CLOBBER_LIST_CC);
    return (unsigned) r;
#define lzo_bitops_cttz64(v)    lzo_bitops_cttz64_func(v)
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_LONG >= 8) && (LZO_WORDSIZE >= 8)
    unsigned r; r = (unsigned) __builtin_ctzl(v); return r;
#define lzo_bitops_cttz64(v)    ((unsigned) __builtin_ctzl(v))
#elif (LZO_BITOPS_USE_GNUC_BITSCAN) && (LZO_SIZEOF_LONG_LONG >= 8) && (LZO_WORDSIZE >= 8)
    unsigned r; r = (unsigned) __builtin_ctzll(v); return r;
#define lzo_bitops_cttz64(v)    ((unsigned) __builtin_ctzll(v))
#else
    LZO_UNUSED(v); return 0;
#endif
}
#endif

lzo_unused_funcs_impl(void, lzo_bitops_unused_funcs)(void)
{
    LZO_UNUSED_FUNC(lzo_bitops_unused_funcs);
    LZO_UNUSED_FUNC(lzo_bitops_ctlz32_func);
    LZO_UNUSED_FUNC(lzo_bitops_cttz32_func);
#if defined(lzo_uint64_t)
    LZO_UNUSED_FUNC(lzo_bitops_ctlz64_func);
    LZO_UNUSED_FUNC(lzo_bitops_cttz64_func);
#endif
}

#if defined(__lzo_alignof) && !(LZO_CFG_NO_UNALIGNED)
#if !defined(lzo_memops_tcheck__) && 0
#define lzo_memops_tcheck__(t,a,b) ((void)0, sizeof(t) == (a) && __lzo_alignof(t) == (b))
#endif
#endif
#ifndef lzo_memops_TU0p
#define lzo_memops_TU0p void __LZO_MMODEL *
#endif
#ifndef lzo_memops_TU1p
#define lzo_memops_TU1p unsigned char __LZO_MMODEL *
#endif
#ifndef lzo_memops_TU2p
#if (LZO_OPT_UNALIGNED16)
typedef lzo_uint16_t __lzo_may_alias lzo_memops_TU2;
#define lzo_memops_TU2p volatile lzo_memops_TU2 *
#elif defined(__lzo_byte_struct)
__lzo_byte_struct(lzo_memops_TU2_struct,2)
typedef struct lzo_memops_TU2_struct lzo_memops_TU2;
#else
struct lzo_memops_TU2_struct { unsigned char a[2]; } __lzo_may_alias;
typedef struct lzo_memops_TU2_struct lzo_memops_TU2;
#endif
#ifndef lzo_memops_TU2p
#define lzo_memops_TU2p lzo_memops_TU2 *
#endif
#endif
#ifndef lzo_memops_TU4p
#if (LZO_OPT_UNALIGNED32)
typedef lzo_uint32_t __lzo_may_alias lzo_memops_TU4;
#define lzo_memops_TU4p volatile lzo_memops_TU4 __LZO_MMODEL *
#elif defined(__lzo_byte_struct)
__lzo_byte_struct(lzo_memops_TU4_struct,4)
typedef struct lzo_memops_TU4_struct lzo_memops_TU4;
#else
struct lzo_memops_TU4_struct { unsigned char a[4]; } __lzo_may_alias;
typedef struct lzo_memops_TU4_struct lzo_memops_TU4;
#endif
#ifndef lzo_memops_TU4p
#define lzo_memops_TU4p lzo_memops_TU4 __LZO_MMODEL *
#endif
#endif
#ifndef lzo_memops_TU8p
#if (LZO_OPT_UNALIGNED64)
typedef lzo_uint64_t __lzo_may_alias lzo_memops_TU8;
#define lzo_memops_TU8p volatile lzo_memops_TU8 __LZO_MMODEL *
#elif defined(__lzo_byte_struct)
__lzo_byte_struct(lzo_memops_TU8_struct,8)
typedef struct lzo_memops_TU8_struct lzo_memops_TU8;
#else
struct lzo_memops_TU8_struct { unsigned char a[8]; } __lzo_may_alias;
typedef struct lzo_memops_TU8_struct lzo_memops_TU8;
#endif
#ifndef lzo_memops_TU8p
#define lzo_memops_TU8p lzo_memops_TU8 __LZO_MMODEL *
#endif
#endif
#ifndef lzo_memops_set_TU1p
#define lzo_memops_set_TU1p     volatile lzo_memops_TU1p
#endif
#ifndef lzo_memops_move_TU1p
#define lzo_memops_move_TU1p    lzo_memops_TU1p
#endif
#define LZO_MEMOPS_SET1(dd,cc) \
    LZO_BLOCK_BEGIN \
    lzo_memops_set_TU1p d__1 = (lzo_memops_set_TU1p) (lzo_memops_TU0p) (dd); \
    d__1[0] = LZO_BYTE(cc); \
    LZO_BLOCK_END
#define LZO_MEMOPS_SET2(dd,cc) \
    LZO_BLOCK_BEGIN \
    lzo_memops_set_TU1p d__2 = (lzo_memops_set_TU1p) (lzo_memops_TU0p) (dd); \
    d__2[0] = LZO_BYTE(cc); d__2[1] = LZO_BYTE(cc); \
    LZO_BLOCK_END
#define LZO_MEMOPS_SET3(dd,cc) \
    LZO_BLOCK_BEGIN \
    lzo_memops_set_TU1p d__3 = (lzo_memops_set_TU1p) (lzo_memops_TU0p) (dd); \
    d__3[0] = LZO_BYTE(cc); d__3[1] = LZO_BYTE(cc); d__3[2] = LZO_BYTE(cc); \
    LZO_BLOCK_END
#define LZO_MEMOPS_SET4(dd,cc) \
    LZO_BLOCK_BEGIN \
    lzo_memops_set_TU1p d__4 = (lzo_memops_set_TU1p) (lzo_memops_TU0p) (dd); \
    d__4[0] = LZO_BYTE(cc); d__4[1] = LZO_BYTE(cc); d__4[2] = LZO_BYTE(cc); d__4[3] = LZO_BYTE(cc); \
    LZO_BLOCK_END
#define LZO_MEMOPS_MOVE1(dd,ss) \
    LZO_BLOCK_BEGIN \
    lzo_memops_move_TU1p d__1 = (lzo_memops_move_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_move_TU1p s__1 = (const lzo_memops_move_TU1p) (const lzo_memops_TU0p) (ss); \
    d__1[0] = s__1[0]; \
    LZO_BLOCK_END
#define LZO_MEMOPS_MOVE2(dd,ss) \
    LZO_BLOCK_BEGIN \
    lzo_memops_move_TU1p d__2 = (lzo_memops_move_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_move_TU1p s__2 = (const lzo_memops_move_TU1p) (const lzo_memops_TU0p) (ss); \
    d__2[0] = s__2[0]; d__2[1] = s__2[1]; \
    LZO_BLOCK_END
#define LZO_MEMOPS_MOVE3(dd,ss) \
    LZO_BLOCK_BEGIN \
    lzo_memops_move_TU1p d__3 = (lzo_memops_move_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_move_TU1p s__3 = (const lzo_memops_move_TU1p) (const lzo_memops_TU0p) (ss); \
    d__3[0] = s__3[0]; d__3[1] = s__3[1]; d__3[2] = s__3[2]; \
    LZO_BLOCK_END
#define LZO_MEMOPS_MOVE4(dd,ss) \
    LZO_BLOCK_BEGIN \
    lzo_memops_move_TU1p d__4 = (lzo_memops_move_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_move_TU1p s__4 = (const lzo_memops_move_TU1p) (const lzo_memops_TU0p) (ss); \
    d__4[0] = s__4[0]; d__4[1] = s__4[1]; d__4[2] = s__4[2]; d__4[3] = s__4[3]; \
    LZO_BLOCK_END
#define LZO_MEMOPS_MOVE8(dd,ss) \
    LZO_BLOCK_BEGIN \
    lzo_memops_move_TU1p d__8 = (lzo_memops_move_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_move_TU1p s__8 = (const lzo_memops_move_TU1p) (const lzo_memops_TU0p) (ss); \
    d__8[0] = s__8[0]; d__8[1] = s__8[1]; d__8[2] = s__8[2]; d__8[3] = s__8[3]; \
    d__8[4] = s__8[4]; d__8[5] = s__8[5]; d__8[6] = s__8[6]; d__8[7] = s__8[7]; \
    LZO_BLOCK_END
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU1p)0)==1)
#define LZO_MEMOPS_COPY1(dd,ss) LZO_MEMOPS_MOVE1(dd,ss)
#if (LZO_OPT_UNALIGNED16)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU2p)0)==2)
#define LZO_MEMOPS_COPY2(dd,ss) \
    * (lzo_memops_TU2p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU2p) (const lzo_memops_TU0p) (ss)
#elif defined(lzo_memops_tcheck__)
#define LZO_MEMOPS_COPY2(dd,ss) \
    LZO_BLOCK_BEGIN if (lzo_memops_tcheck__(lzo_memops_TU2,2,1)) { \
        * (lzo_memops_TU2p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU2p) (const lzo_memops_TU0p) (ss); \
    } else { LZO_MEMOPS_MOVE2(dd,ss); } LZO_BLOCK_END
#else
#define LZO_MEMOPS_COPY2(dd,ss) LZO_MEMOPS_MOVE2(dd,ss)
#endif
#if (LZO_OPT_UNALIGNED32)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU4p)0)==4)
#define LZO_MEMOPS_COPY4(dd,ss) \
    * (lzo_memops_TU4p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU4p) (const lzo_memops_TU0p) (ss)
#elif defined(lzo_memops_tcheck__)
#define LZO_MEMOPS_COPY4(dd,ss) \
    LZO_BLOCK_BEGIN if (lzo_memops_tcheck__(lzo_memops_TU4,4,1)) { \
        * (lzo_memops_TU4p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU4p) (const lzo_memops_TU0p) (ss); \
    } else { LZO_MEMOPS_MOVE4(dd,ss); } LZO_BLOCK_END
#else
#define LZO_MEMOPS_COPY4(dd,ss) LZO_MEMOPS_MOVE4(dd,ss)
#endif
#if (LZO_WORDSIZE != 8)
#define LZO_MEMOPS_COPY8(dd,ss) \
    LZO_BLOCK_BEGIN LZO_MEMOPS_COPY4(dd,ss); LZO_MEMOPS_COPY4((lzo_memops_TU1p)(lzo_memops_TU0p)(dd)+4,(const lzo_memops_TU1p)(const lzo_memops_TU0p)(ss)+4); LZO_BLOCK_END
#else
#if (LZO_OPT_UNALIGNED64)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU8p)0)==8)
#define LZO_MEMOPS_COPY8(dd,ss) \
    * (lzo_memops_TU8p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU8p) (const lzo_memops_TU0p) (ss)
#elif (LZO_OPT_UNALIGNED32)
#define LZO_MEMOPS_COPY8(dd,ss) \
    LZO_BLOCK_BEGIN LZO_MEMOPS_COPY4(dd,ss); LZO_MEMOPS_COPY4((lzo_memops_TU1p)(lzo_memops_TU0p)(dd)+4,(const lzo_memops_TU1p)(const lzo_memops_TU0p)(ss)+4); LZO_BLOCK_END
#elif defined(lzo_memops_tcheck__)
#define LZO_MEMOPS_COPY8(dd,ss) \
    LZO_BLOCK_BEGIN if (lzo_memops_tcheck__(lzo_memops_TU8,8,1)) { \
        * (lzo_memops_TU8p) (lzo_memops_TU0p) (dd) = * (const lzo_memops_TU8p) (const lzo_memops_TU0p) (ss); \
    } else { LZO_MEMOPS_MOVE8(dd,ss); } LZO_BLOCK_END
#else
#define LZO_MEMOPS_COPY8(dd,ss) LZO_MEMOPS_MOVE8(dd,ss)
#endif
#endif
#define LZO_MEMOPS_COPYN(dd,ss,nn) \
    LZO_BLOCK_BEGIN \
    lzo_memops_TU1p d__n = (lzo_memops_TU1p) (lzo_memops_TU0p) (dd); \
    const lzo_memops_TU1p s__n = (const lzo_memops_TU1p) (const lzo_memops_TU0p) (ss); \
    lzo_uint n__n = (nn); \
    while ((void)0, n__n >= 8) { LZO_MEMOPS_COPY8(d__n, s__n); d__n += 8; s__n += 8; n__n -= 8; } \
    if ((void)0, n__n >= 4) { LZO_MEMOPS_COPY4(d__n, s__n); d__n += 4; s__n += 4; n__n -= 4; } \
    if ((void)0, n__n > 0) do { *d__n++ = *s__n++; } while (--n__n > 0); \
    LZO_BLOCK_END

__lzo_static_forceinline lzo_uint16_t lzo_memops_get_le16(const lzo_voidp ss)
{
    lzo_uint16_t v;
#if (LZO_ABI_LITTLE_ENDIAN)
    LZO_MEMOPS_COPY2(&v, ss);
#elif (LZO_OPT_UNALIGNED16 && LZO_ARCH_POWERPC && LZO_ABI_BIG_ENDIAN) && (LZO_ASM_SYNTAX_GNUC)
    const lzo_memops_TU2p s = (const lzo_memops_TU2p) ss;
    unsigned long vv;
    __asm__("lhbrx %0,0,%1" : "=r" (vv) : "r" (s), "m" (*s));
    v = (lzo_uint16_t) vv;
#else
    const lzo_memops_TU1p s = (const lzo_memops_TU1p) ss;
    v = (lzo_uint16_t) (((lzo_uint16_t)s[0]) | ((lzo_uint16_t)s[1] << 8));
#endif
    return v;
}
#if (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
#define LZO_MEMOPS_GET_LE16(ss)    (* (const lzo_memops_TU2p) (const lzo_memops_TU0p) (ss))
#else
#define LZO_MEMOPS_GET_LE16(ss)    lzo_memops_get_le16(ss)
#endif

__lzo_static_forceinline lzo_uint32_t lzo_memops_get_le32(const lzo_voidp ss)
{
    lzo_uint32_t v;
#if (LZO_ABI_LITTLE_ENDIAN)
    LZO_MEMOPS_COPY4(&v, ss);
#elif (LZO_OPT_UNALIGNED32 && LZO_ARCH_POWERPC && LZO_ABI_BIG_ENDIAN) && (LZO_ASM_SYNTAX_GNUC)
    const lzo_memops_TU4p s = (const lzo_memops_TU4p) ss;
    unsigned long vv;
    __asm__("lwbrx %0,0,%1" : "=r" (vv) : "r" (s), "m" (*s));
    v = (lzo_uint32_t) vv;
#else
    const lzo_memops_TU1p s = (const lzo_memops_TU1p) ss;
    v = (lzo_uint32_t) (((lzo_uint32_t)s[0]) | ((lzo_uint32_t)s[1] << 8) | ((lzo_uint32_t)s[2] << 16) | ((lzo_uint32_t)s[3] << 24));
#endif
    return v;
}
#if (LZO_OPT_UNALIGNED32) && (LZO_ABI_LITTLE_ENDIAN)
#define LZO_MEMOPS_GET_LE32(ss)    (* (const lzo_memops_TU4p) (const lzo_memops_TU0p) (ss))
#else
#define LZO_MEMOPS_GET_LE32(ss)    lzo_memops_get_le32(ss)
#endif

#if (LZO_OPT_UNALIGNED64) && (LZO_ABI_LITTLE_ENDIAN)
#define LZO_MEMOPS_GET_LE64(ss)    (* (const lzo_memops_TU8p) (const lzo_memops_TU0p) (ss))
#endif

__lzo_static_forceinline lzo_uint16_t lzo_memops_get_ne16(const lzo_voidp ss)
{
    lzo_uint16_t v;
    LZO_MEMOPS_COPY2(&v, ss);
    return v;
}
#if (LZO_OPT_UNALIGNED16)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU2p)0)==2)
#define LZO_MEMOPS_GET_NE16(ss)    (* (const lzo_memops_TU2p) (const lzo_memops_TU0p) (ss))
#else
#define LZO_MEMOPS_GET_NE16(ss)    lzo_memops_get_ne16(ss)
#endif

__lzo_static_forceinline lzo_uint32_t lzo_memops_get_ne32(const lzo_voidp ss)
{
    lzo_uint32_t v;
    LZO_MEMOPS_COPY4(&v, ss);
    return v;
}
#if (LZO_OPT_UNALIGNED32)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU4p)0)==4)
#define LZO_MEMOPS_GET_NE32(ss)    (* (const lzo_memops_TU4p) (const lzo_memops_TU0p) (ss))
#else
#define LZO_MEMOPS_GET_NE32(ss)    lzo_memops_get_ne32(ss)
#endif

#if (LZO_OPT_UNALIGNED64)
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(*(lzo_memops_TU8p)0)==8)
#define LZO_MEMOPS_GET_NE64(ss)    (* (const lzo_memops_TU8p) (const lzo_memops_TU0p) (ss))
#endif

__lzo_static_forceinline void lzo_memops_put_le16(lzo_voidp dd, lzo_uint16_t vv)
{
#if (LZO_ABI_LITTLE_ENDIAN)
    LZO_MEMOPS_COPY2(dd, &vv);
#elif (LZO_OPT_UNALIGNED16 && LZO_ARCH_POWERPC && LZO_ABI_BIG_ENDIAN) && (LZO_ASM_SYNTAX_GNUC)
    lzo_memops_TU2p d = (lzo_memops_TU2p) dd;
    unsigned long v = vv;
    __asm__("sthbrx %2,0,%1" : "=m" (*d) : "r" (d), "r" (v));
#else
    lzo_memops_TU1p d = (lzo_memops_TU1p) dd;
    d[0] = LZO_BYTE((vv      ) & 0xff);
    d[1] = LZO_BYTE((vv >>  8) & 0xff);
#endif
}
#if (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
#define LZO_MEMOPS_PUT_LE16(dd,vv) (* (lzo_memops_TU2p) (lzo_memops_TU0p) (dd) = (vv))
#else
#define LZO_MEMOPS_PUT_LE16(dd,vv) lzo_memops_put_le16(dd,vv)
#endif

__lzo_static_forceinline void lzo_memops_put_le32(lzo_voidp dd, lzo_uint32_t vv)
{
#if (LZO_ABI_LITTLE_ENDIAN)
    LZO_MEMOPS_COPY4(dd, &vv);
#elif (LZO_OPT_UNALIGNED32 && LZO_ARCH_POWERPC && LZO_ABI_BIG_ENDIAN) && (LZO_ASM_SYNTAX_GNUC)
    lzo_memops_TU4p d = (lzo_memops_TU4p) dd;
    unsigned long v = vv;
    __asm__("stwbrx %2,0,%1" : "=m" (*d) : "r" (d), "r" (v));
#else
    lzo_memops_TU1p d = (lzo_memops_TU1p) dd;
    d[0] = LZO_BYTE((vv      ) & 0xff);
    d[1] = LZO_BYTE((vv >>  8) & 0xff);
    d[2] = LZO_BYTE((vv >> 16) & 0xff);
    d[3] = LZO_BYTE((vv >> 24) & 0xff);
#endif
}
#if (LZO_OPT_UNALIGNED32) && (LZO_ABI_LITTLE_ENDIAN)
#define LZO_MEMOPS_PUT_LE32(dd,vv) (* (lzo_memops_TU4p) (lzo_memops_TU0p) (dd) = (vv))
#else
#define LZO_MEMOPS_PUT_LE32(dd,vv) lzo_memops_put_le32(dd,vv)
#endif

__lzo_static_forceinline void lzo_memops_put_ne16(lzo_voidp dd, lzo_uint16_t vv)
{
    LZO_MEMOPS_COPY2(dd, &vv);
}
#if (LZO_OPT_UNALIGNED16)
#define LZO_MEMOPS_PUT_NE16(dd,vv) (* (lzo_memops_TU2p) (lzo_memops_TU0p) (dd) = (vv))
#else
#define LZO_MEMOPS_PUT_NE16(dd,vv) lzo_memops_put_ne16(dd,vv)
#endif

__lzo_static_forceinline void lzo_memops_put_ne32(lzo_voidp dd, lzo_uint32_t vv)
{
    LZO_MEMOPS_COPY4(dd, &vv);
}
#if (LZO_OPT_UNALIGNED32)
#define LZO_MEMOPS_PUT_NE32(dd,vv) (* (lzo_memops_TU4p) (lzo_memops_TU0p) (dd) = (vv))
#else
#define LZO_MEMOPS_PUT_NE32(dd,vv) lzo_memops_put_ne32(dd,vv)
#endif

lzo_unused_funcs_impl(void, lzo_memops_unused_funcs)(void)
{
    LZO_UNUSED_FUNC(lzo_memops_unused_funcs);
    LZO_UNUSED_FUNC(lzo_memops_get_le16);
    LZO_UNUSED_FUNC(lzo_memops_get_le32);
    LZO_UNUSED_FUNC(lzo_memops_get_ne16);
    LZO_UNUSED_FUNC(lzo_memops_get_ne32);
    LZO_UNUSED_FUNC(lzo_memops_put_le16);
    LZO_UNUSED_FUNC(lzo_memops_put_le32);
    LZO_UNUSED_FUNC(lzo_memops_put_ne16);
    LZO_UNUSED_FUNC(lzo_memops_put_ne32);
}

#endif

#ifndef UA_SET1
#define UA_SET1             LZO_MEMOPS_SET1
#endif
#ifndef UA_SET2
#define UA_SET2             LZO_MEMOPS_SET2
#endif
#ifndef UA_SET3
#define UA_SET3             LZO_MEMOPS_SET3
#endif
#ifndef UA_SET4
#define UA_SET4             LZO_MEMOPS_SET4
#endif
#ifndef UA_MOVE1
#define UA_MOVE1            LZO_MEMOPS_MOVE1
#endif
#ifndef UA_MOVE2
#define UA_MOVE2            LZO_MEMOPS_MOVE2
#endif
#ifndef UA_MOVE3
#define UA_MOVE3            LZO_MEMOPS_MOVE3
#endif
#ifndef UA_MOVE4
#define UA_MOVE4            LZO_MEMOPS_MOVE4
#endif
#ifndef UA_MOVE8
#define UA_MOVE8            LZO_MEMOPS_MOVE8
#endif
#ifndef UA_COPY1
#define UA_COPY1            LZO_MEMOPS_COPY1
#endif
#ifndef UA_COPY2
#define UA_COPY2            LZO_MEMOPS_COPY2
#endif
#ifndef UA_COPY3
#define UA_COPY3            LZO_MEMOPS_COPY3
#endif
#ifndef UA_COPY4
#define UA_COPY4            LZO_MEMOPS_COPY4
#endif
#ifndef UA_COPY8
#define UA_COPY8            LZO_MEMOPS_COPY8
#endif
#ifndef UA_COPYN
#define UA_COPYN            LZO_MEMOPS_COPYN
#endif
#ifndef UA_COPYN_X
#define UA_COPYN_X          LZO_MEMOPS_COPYN
#endif
#ifndef UA_GET_LE16
#define UA_GET_LE16         LZO_MEMOPS_GET_LE16
#endif
#ifndef UA_GET_LE32
#define UA_GET_LE32         LZO_MEMOPS_GET_LE32
#endif
#ifdef LZO_MEMOPS_GET_LE64
#ifndef UA_GET_LE64
#define UA_GET_LE64         LZO_MEMOPS_GET_LE64
#endif
#endif
#ifndef UA_GET_NE16
#define UA_GET_NE16         LZO_MEMOPS_GET_NE16
#endif
#ifndef UA_GET_NE32
#define UA_GET_NE32         LZO_MEMOPS_GET_NE32
#endif
#ifdef LZO_MEMOPS_GET_NE64
#ifndef UA_GET_NE64
#define UA_GET_NE64         LZO_MEMOPS_GET_NE64
#endif
#endif
#ifndef UA_PUT_LE16
#define UA_PUT_LE16         LZO_MEMOPS_PUT_LE16
#endif
#ifndef UA_PUT_LE32
#define UA_PUT_LE32         LZO_MEMOPS_PUT_LE32
#endif
#ifndef UA_PUT_NE16
#define UA_PUT_NE16         LZO_MEMOPS_PUT_NE16
#endif
#ifndef UA_PUT_NE32
#define UA_PUT_NE32         LZO_MEMOPS_PUT_NE32
#endif

#define MEMCPY8_DS(dest,src,len) \
    lzo_memcpy(dest,src,len); dest += len; src += len

#define BZERO8_PTR(s,l,n) \
    lzo_memset((lzo_voidp)(s),0,(lzo_uint)(l)*(n))

#define MEMCPY_DS(dest,src,len) \
    do *dest++ = *src++; while (--len > 0)

LZO_EXTERN(const lzo_bytep) lzo_copyright(void);

#ifndef __LZO_PTR_H
#define __LZO_PTR_H 1

#ifdef __cplusplus
extern "C" {
#endif

#if (LZO_ARCH_I086)
#error "LZO_ARCH_I086 is unsupported"
#elif (LZO_MM_PVP)
#error "LZO_MM_PVP is unsupported"
#else
#define PTR(a)              ((lzo_uintptr_t) (a))
#define PTR_LINEAR(a)       PTR(a)
#define PTR_ALIGNED_4(a)    ((PTR_LINEAR(a) & 3) == 0)
#define PTR_ALIGNED_8(a)    ((PTR_LINEAR(a) & 7) == 0)
#define PTR_ALIGNED2_4(a,b) (((PTR_LINEAR(a) | PTR_LINEAR(b)) & 3) == 0)
#define PTR_ALIGNED2_8(a,b) (((PTR_LINEAR(a) | PTR_LINEAR(b)) & 7) == 0)
#endif

#define PTR_LT(a,b)         (PTR(a) < PTR(b))
#define PTR_GE(a,b)         (PTR(a) >= PTR(b))
#define PTR_DIFF(a,b)       (PTR(a) - PTR(b))
#define pd(a,b)             ((lzo_uint) ((a)-(b)))

LZO_EXTERN(lzo_uintptr_t)
__lzo_ptr_linear(const lzo_voidp ptr);

typedef union
{
    char            a_char;
    unsigned char   a_uchar;
    short           a_short;
    unsigned short  a_ushort;
    int             a_int;
    unsigned int    a_uint;
    long            a_long;
    unsigned long   a_ulong;
    lzo_int         a_lzo_int;
    lzo_uint        a_lzo_uint;
    lzo_xint        a_lzo_xint;
    lzo_int16_t     a_lzo_int16_t;
    lzo_uint16_t    a_lzo_uint16_t;
    lzo_int32_t     a_lzo_int32_t;
    lzo_uint32_t    a_lzo_uint32_t;
#if defined(lzo_uint64_t)
    lzo_int64_t     a_lzo_int64_t;
    lzo_uint64_t    a_lzo_uint64_t;
#endif
    size_t          a_size_t;
    ptrdiff_t       a_ptrdiff_t;
    lzo_uintptr_t   a_lzo_uintptr_t;
    void *          a_void_p;
    char *          a_char_p;
    unsigned char * a_uchar_p;
    const void *          a_c_void_p;
    const char *          a_c_char_p;
    const unsigned char * a_c_uchar_p;
    lzo_voidp       a_lzo_voidp;
    lzo_bytep       a_lzo_bytep;
    const lzo_voidp a_c_lzo_voidp;
    const lzo_bytep a_c_lzo_bytep;
}
lzo_full_align_t;

#ifdef __cplusplus
}
#endif

#endif

#ifndef LZO_DETERMINISTIC
#define LZO_DETERMINISTIC 1
#endif

#ifndef LZO_DICT_USE_PTR
#define LZO_DICT_USE_PTR 1
#endif

#if (LZO_DICT_USE_PTR)
#  define lzo_dict_t    const lzo_bytep
#  define lzo_dict_p    lzo_dict_t *
#else
#  define lzo_dict_t    lzo_uint
#  define lzo_dict_p    lzo_dict_t *
#endif

#endif

#if !defined(MINILZO_CFG_SKIP_LZO_PTR)

LZO_PUBLIC(lzo_uintptr_t)
__lzo_ptr_linear(const lzo_voidp ptr)
{
    lzo_uintptr_t p;

#if (LZO_ARCH_I086)
#error "LZO_ARCH_I086 is unsupported"
#elif (LZO_MM_PVP)
#error "LZO_MM_PVP is unsupported"
#else
    p = (lzo_uintptr_t) PTR_LINEAR(ptr);
#endif

    return p;
}

LZO_PUBLIC(unsigned)
__lzo_align_gap(const lzo_voidp ptr, lzo_uint size)
{
#if (__LZO_UINTPTR_T_IS_POINTER)
#error "__LZO_UINTPTR_T_IS_POINTER is unsupported"
#else
    lzo_uintptr_t p, n;
    if (size < 2) return 0;
    p = __lzo_ptr_linear(ptr);
#if 0
    n = (((p + size - 1) / size) * size) - p;
#else
    if ((size & (size - 1)) != 0)
        return 0;
    n = size; n = ((p + n - 1) & ~(n - 1)) - p;
#endif
#endif
    assert((long)n >= 0);
    assert(n <= size);
    return (unsigned)n;
}

#endif
#if !defined(MINILZO_CFG_SKIP_LZO_UTIL)

/* If you use the LZO library in a product, I would appreciate that you
 * keep this copyright string in the executable of your product.
 */

static const char lzo_copyright_[] =
#if !defined(__LZO_IN_MINLZO)
    LZO_VERSION_STRING;
#else
    "\r\n\n"
    "LZO data compression library.\n"
    "$Copyright: LZO Copyright (C) 1996-2015 Markus Franz Xaver Johannes Oberhumer\n"
    "<markus@oberhumer.com>\n"
    "http://www.oberhumer.com $\n\n"
    "$Id: LZO version: v" LZO_VERSION_STRING ", " LZO_VERSION_DATE " $\n"
    "$Info: " LZO_INFO_STRING " $\n";
#endif
static const char lzo_version_string_[] = LZO_VERSION_STRING;
static const char lzo_version_date_[] = LZO_VERSION_DATE;

LZO_PUBLIC(const lzo_bytep)
lzo_copyright(void)
{
    return (const lzo_bytep) lzo_copyright_;
}

LZO_PUBLIC(unsigned)
lzo_version(void)
{
    return LZO_VERSION;
}

LZO_PUBLIC(const char *)
lzo_version_string(void)
{
    return lzo_version_string_;
}

LZO_PUBLIC(const char *)
lzo_version_date(void)
{
    return lzo_version_date_;
}

LZO_PUBLIC(const lzo_charp)
_lzo_version_string(void)
{
    return lzo_version_string_;
}

LZO_PUBLIC(const lzo_charp)
_lzo_version_date(void)
{
    return lzo_version_date_;
}

#define LZO_BASE 65521u
#define LZO_NMAX 5552

#define LZO_DO1(buf,i)  s1 += buf[i]; s2 += s1
#define LZO_DO2(buf,i)  LZO_DO1(buf,i); LZO_DO1(buf,i+1)
#define LZO_DO4(buf,i)  LZO_DO2(buf,i); LZO_DO2(buf,i+2)
#define LZO_DO8(buf,i)  LZO_DO4(buf,i); LZO_DO4(buf,i+4)
#define LZO_DO16(buf,i) LZO_DO8(buf,i); LZO_DO8(buf,i+8)

LZO_PUBLIC(lzo_uint32_t)
lzo_adler32(lzo_uint32_t adler, const lzo_bytep buf, lzo_uint len)
{
    lzo_uint32_t s1 = adler & 0xffff;
    lzo_uint32_t s2 = (adler >> 16) & 0xffff;
    unsigned k;

    if (buf == NULL)
        return 1;

    while (len > 0)
    {
        k = len < LZO_NMAX ? (unsigned) len : LZO_NMAX;
        len -= k;
        if (k >= 16) do
        {
            LZO_DO16(buf,0);
            buf += 16;
            k -= 16;
        } while (k >= 16);
        if (k != 0) do
        {
            s1 += *buf++;
            s2 += s1;
        } while (--k > 0);
        s1 %= LZO_BASE;
        s2 %= LZO_BASE;
    }
    return (s2 << 16) | s1;
}

#undef LZO_DO1
#undef LZO_DO2
#undef LZO_DO4
#undef LZO_DO8
#undef LZO_DO16

#endif
#if !defined(MINILZO_CFG_SKIP_LZO_STRING)
#undef lzo_memcmp
#undef lzo_memcpy
#undef lzo_memmove
#undef lzo_memset
#if !defined(__LZO_MMODEL_HUGE)
#  undef LZO_HAVE_MM_HUGE_PTR
#endif
#define lzo_hsize_t             lzo_uint
#define lzo_hvoid_p             lzo_voidp
#define lzo_hbyte_p             lzo_bytep
#define LZOLIB_PUBLIC(r,f)      LZO_PUBLIC(r) f
#define lzo_hmemcmp             lzo_memcmp
#define lzo_hmemcpy             lzo_memcpy
#define lzo_hmemmove            lzo_memmove
#define lzo_hmemset             lzo_memset
#define __LZOLIB_HMEMCPY_CH_INCLUDED 1
#if !defined(LZOLIB_PUBLIC)
#  define LZOLIB_PUBLIC(r,f)    r __LZOLIB_FUNCNAME(f)
#endif
LZOLIB_PUBLIC(int, lzo_hmemcmp) (const lzo_hvoid_p s1, const lzo_hvoid_p s2, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !(HAVE_MEMCMP)
    const lzo_hbyte_p p1 = LZO_STATIC_CAST(const lzo_hbyte_p, s1);
    const lzo_hbyte_p p2 = LZO_STATIC_CAST(const lzo_hbyte_p, s2);
    if __lzo_likely(len > 0) do
    {
        int d = *p1 - *p2;
        if (d != 0)
            return d;
        p1++; p2++;
    } while __lzo_likely(--len > 0);
    return 0;
#else
    return memcmp(s1, s2, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemcpy) (lzo_hvoid_p dest, const lzo_hvoid_p src, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !(HAVE_MEMCPY)
    lzo_hbyte_p p1 = LZO_STATIC_CAST(lzo_hbyte_p, dest);
    const lzo_hbyte_p p2 = LZO_STATIC_CAST(const lzo_hbyte_p, src);
    if (!(len > 0) || p1 == p2)
        return dest;
    do
        *p1++ = *p2++;
    while __lzo_likely(--len > 0);
    return dest;
#else
    return memcpy(dest, src, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemmove) (lzo_hvoid_p dest, const lzo_hvoid_p src, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !(HAVE_MEMMOVE)
    lzo_hbyte_p p1 = LZO_STATIC_CAST(lzo_hbyte_p, dest);
    const lzo_hbyte_p p2 = LZO_STATIC_CAST(const lzo_hbyte_p, src);
    if (!(len > 0) || p1 == p2)
        return dest;
    if (p1 < p2)
    {
        do
            *p1++ = *p2++;
        while __lzo_likely(--len > 0);
    }
    else
    {
        p1 += len;
        p2 += len;
        do
            *--p1 = *--p2;
        while __lzo_likely(--len > 0);
    }
    return dest;
#else
    return memmove(dest, src, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemset) (lzo_hvoid_p s, int cc, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !(HAVE_MEMSET)
    lzo_hbyte_p p = LZO_STATIC_CAST(lzo_hbyte_p, s);
    unsigned char c = LZO_ITRUNC(unsigned char, cc);
    if __lzo_likely(len > 0) do
        *p++ = c;
    while __lzo_likely(--len > 0);
    return s;
#else
    return memset(s, cc, len);
#endif
}
#undef LZOLIB_PUBLIC
#endif
#if !defined(MINILZO_CFG_SKIP_LZO_INIT)

#if !defined(__LZO_IN_MINILZO)

#define LZO_WANT_ACC_CHK_CH 1
#undef LZOCHK_ASSERT

    LZOCHK_ASSERT((LZO_UINT32_C(1) << (int)(8*sizeof(LZO_UINT32_C(1))-1)) > 0)
    LZOCHK_ASSERT_IS_SIGNED_T(lzo_int)
    LZOCHK_ASSERT_IS_UNSIGNED_T(lzo_uint)
#if !(__LZO_UINTPTR_T_IS_POINTER)
    LZOCHK_ASSERT_IS_UNSIGNED_T(lzo_uintptr_t)
#endif
    LZOCHK_ASSERT(sizeof(lzo_uintptr_t) >= sizeof(lzo_voidp))
    LZOCHK_ASSERT_IS_UNSIGNED_T(lzo_xint)

#endif
#undef LZOCHK_ASSERT

union lzo_config_check_union {
    lzo_uint a[2];
    unsigned char b[2*LZO_MAX(8,sizeof(lzo_uint))];
#if defined(lzo_uint64_t)
    lzo_uint64_t c[2];
#endif
};

#if 0
#define u2p(ptr,off) ((lzo_voidp) (((lzo_bytep)(lzo_voidp)(ptr)) + (off)))
#else
static __lzo_noinline lzo_voidp u2p(lzo_voidp ptr, lzo_uint off)
{
    return (lzo_voidp) ((lzo_bytep) ptr + off);
}
#endif

LZO_PUBLIC(int)
_lzo_config_check(void)
{
#if (LZO_CC_CLANG && (LZO_CC_CLANG >= 0x030100ul && LZO_CC_CLANG < 0x030300ul))
# if 0
    volatile
# endif
#endif
    union lzo_config_check_union u;
    lzo_voidp p;
    unsigned r = 1;

    u.a[0] = u.a[1] = 0;
    p = u2p(&u, 0);
    r &= ((* (lzo_bytep) p) == 0);
#if !(LZO_CFG_NO_CONFIG_CHECK)
#if (LZO_ABI_BIG_ENDIAN)
    u.a[0] = u.a[1] = 0; u.b[sizeof(lzo_uint) - 1] = 128;
    p = u2p(&u, 0);
    r &= ((* (lzo_uintp) p) == 128);
#endif
#if (LZO_ABI_LITTLE_ENDIAN)
    u.a[0] = u.a[1] = 0; u.b[0] = 128;
    p = u2p(&u, 0);
    r &= ((* (lzo_uintp) p) == 128);
#endif
    u.a[0] = u.a[1] = 0;
    u.b[0] = 1; u.b[3] = 2;
    p = u2p(&u, 1);
    r &= UA_GET_NE16(p) == 0;
    r &= UA_GET_LE16(p) == 0;
    u.b[1] = 128;
    r &= UA_GET_LE16(p) == 128;
    u.b[2] = 129;
    r &= UA_GET_LE16(p) == LZO_UINT16_C(0x8180);
#if (LZO_ABI_BIG_ENDIAN)
    r &= UA_GET_NE16(p) == LZO_UINT16_C(0x8081);
#endif
#if (LZO_ABI_LITTLE_ENDIAN)
    r &= UA_GET_NE16(p) == LZO_UINT16_C(0x8180);
#endif
    u.a[0] = u.a[1] = 0;
    u.b[0] = 3; u.b[5] = 4;
    p = u2p(&u, 1);
    r &= UA_GET_NE32(p) == 0;
    r &= UA_GET_LE32(p) == 0;
    u.b[1] = 128;
    r &= UA_GET_LE32(p) == 128;
    u.b[2] = 129; u.b[3] = 130; u.b[4] = 131;
    r &= UA_GET_LE32(p) == LZO_UINT32_C(0x83828180);
#if (LZO_ABI_BIG_ENDIAN)
    r &= UA_GET_NE32(p) == LZO_UINT32_C(0x80818283);
#endif
#if (LZO_ABI_LITTLE_ENDIAN)
    r &= UA_GET_NE32(p) == LZO_UINT32_C(0x83828180);
#endif
#if defined(UA_GET_NE64)
    u.c[0] = u.c[1] = 0;
    u.b[0] = 5; u.b[9] = 6;
    p = u2p(&u, 1);
    u.c[0] = u.c[1] = 0;
    r &= UA_GET_NE64(p) == 0;
#if defined(UA_GET_LE64)
    r &= UA_GET_LE64(p) == 0;
    u.b[1] = 128;
    r &= UA_GET_LE64(p) == 128;
#endif
#endif
#if defined(lzo_bitops_ctlz32)
    { unsigned i = 0; lzo_uint32_t v;
    for (v = 1; v != 0 && r == 1; v <<= 1, i++) {
        r &= lzo_bitops_ctlz32(v) == 31 - i;
        r &= lzo_bitops_ctlz32_func(v) == 31 - i;
    }}
#endif
#if defined(lzo_bitops_ctlz64)
    { unsigned i = 0; lzo_uint64_t v;
    for (v = 1; v != 0 && r == 1; v <<= 1, i++) {
        r &= lzo_bitops_ctlz64(v) == 63 - i;
        r &= lzo_bitops_ctlz64_func(v) == 63 - i;
    }}
#endif
#if defined(lzo_bitops_cttz32)
    { unsigned i = 0; lzo_uint32_t v;
    for (v = 1; v != 0 && r == 1; v <<= 1, i++) {
        r &= lzo_bitops_cttz32(v) == i;
        r &= lzo_bitops_cttz32_func(v) == i;
    }}
#endif
#if defined(lzo_bitops_cttz64)
    { unsigned i = 0; lzo_uint64_t v;
    for (v = 1; v != 0 && r == 1; v <<= 1, i++) {
        r &= lzo_bitops_cttz64(v) == i;
        r &= lzo_bitops_cttz64_func(v) == i;
    }}
#endif
#endif
    LZO_UNUSED_FUNC(lzo_bitops_unused_funcs);

    return r == 1 ? LZO_E_OK : LZO_E_ERROR;
}

LZO_PUBLIC(int)
__lzo_init_v2(unsigned v, int s1, int s2, int s3, int s4, int s5,
                          int s6, int s7, int s8, int s9)
{
    int r;

#if defined(__LZO_IN_MINILZO)
#elif (LZO_CC_MSC && ((_MSC_VER) < 700))
#else
#define LZO_WANT_ACC_CHK_CH 1
#undef LZOCHK_ASSERT
#define LZOCHK_ASSERT(expr)  LZO_COMPILE_TIME_ASSERT(expr)
#endif
#undef LZOCHK_ASSERT

    if (v == 0)
        return LZO_E_ERROR;

    r = (s1 == -1 || s1 == (int) sizeof(short)) &&
        (s2 == -1 || s2 == (int) sizeof(int)) &&
        (s3 == -1 || s3 == (int) sizeof(long)) &&
        (s4 == -1 || s4 == (int) sizeof(lzo_uint32_t)) &&
        (s5 == -1 || s5 == (int) sizeof(lzo_uint)) &&
        (s6 == -1 || s6 == (int) lzo_sizeof_dict_t) &&
        (s7 == -1 || s7 == (int) sizeof(char *)) &&
        (s8 == -1 || s8 == (int) sizeof(lzo_voidp)) &&
        (s9 == -1 || s9 == (int) sizeof(lzo_callback_t));
    if (!r)
        return LZO_E_ERROR;

    r = _lzo_config_check();
    if (r != LZO_E_OK)
        return r;

    return r;
}

#if !defined(__LZO_IN_MINILZO)

#if (LZO_OS_WIN16 && LZO_CC_WATCOMC) && defined(__SW_BD)

#if 0
BOOL FAR PASCAL LibMain ( HANDLE hInstance, WORD wDataSegment,
                          WORD wHeapSize, LPSTR lpszCmdLine )
#else
int __far __pascal LibMain ( int a, short b, short c, long d )
#endif
{
    LZO_UNUSED(a); LZO_UNUSED(b); LZO_UNUSED(c); LZO_UNUSED(d);
    return 1;
}

#endif

#endif

#endif

#define LZO1X           1
#define LZO_EOF_CODE    1
#define M2_MAX_OFFSET   0x0800

#if !defined(MINILZO_CFG_SKIP_LZO1X_1_COMPRESS)

#if 1 && defined(UA_GET_LE32)
#undef  LZO_DICT_USE_PTR
#define LZO_DICT_USE_PTR 0
#undef  lzo_dict_t
#define lzo_dict_t lzo_uint16_t
#endif

#define LZO_NEED_DICT_H 1
#ifndef D_BITS
#define D_BITS          14
#endif
#define D_INDEX1(d,p)       d = DM(DMUL(0x21,DX3(p,5,5,6)) >> 5)
#define D_INDEX2(d,p)       d = (d & (D_MASK & 0x7ff)) ^ (D_HIGH | 0x1f)
#if 1
#define DINDEX(dv,p)        DM(((DMUL(0x1824429d,dv)) >> (32-D_BITS)))
#else
#define DINDEX(dv,p)        DM((dv) + ((dv) >> (32-D_BITS)))
#endif

#ifndef __LZO_CONFIG1X_H
#define __LZO_CONFIG1X_H 1

#if !defined(LZO1X) && !defined(LZO1Y) && !defined(LZO1Z)
#  define LZO1X 1
#endif

#if !defined(__LZO_IN_MINILZO)
#include <lzo/lzo1x.h>
#endif

#ifndef LZO_EOF_CODE
#define LZO_EOF_CODE 1
#endif
#undef LZO_DETERMINISTIC

#define M1_MAX_OFFSET   0x0400
#ifndef M2_MAX_OFFSET
#define M2_MAX_OFFSET   0x0800
#endif
#define M3_MAX_OFFSET   0x4000
#define M4_MAX_OFFSET   0xbfff

#define MX_MAX_OFFSET   (M1_MAX_OFFSET + M2_MAX_OFFSET)

#define M1_MIN_LEN      2
#define M1_MAX_LEN      2
#define M2_MIN_LEN      3
#ifndef M2_MAX_LEN
#define M2_MAX_LEN      8
#endif
#define M3_MIN_LEN      3
#define M3_MAX_LEN      33
#define M4_MIN_LEN      3
#define M4_MAX_LEN      9

#define M1_MARKER       0
#define M2_MARKER       64
#define M3_MARKER       32
#define M4_MARKER       16

#ifndef MIN_LOOKAHEAD
#define MIN_LOOKAHEAD       (M2_MAX_LEN + 1)
#endif

#if defined(LZO_NEED_DICT_H)

#ifndef LZO_HASH
#define LZO_HASH            LZO_HASH_LZO_INCREMENTAL_B
#endif
#define DL_MIN_LEN          M2_MIN_LEN

#ifndef __LZO_DICT_H
#define __LZO_DICT_H 1

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(D_BITS) && defined(DBITS)
#  define D_BITS        DBITS
#endif
#if !defined(D_BITS)
#  error "D_BITS is not defined"
#endif
#if (D_BITS < 16)
#  define D_SIZE        LZO_SIZE(D_BITS)
#  define D_MASK        LZO_MASK(D_BITS)
#else
#  define D_SIZE        LZO_USIZE(D_BITS)
#  define D_MASK        LZO_UMASK(D_BITS)
#endif
#define D_HIGH          ((D_MASK >> 1) + 1)

#if !defined(DD_BITS)
#  define DD_BITS       0
#endif
#define DD_SIZE         LZO_SIZE(DD_BITS)
#define DD_MASK         LZO_MASK(DD_BITS)

#if !defined(DL_BITS)
#  define DL_BITS       (D_BITS - DD_BITS)
#endif
#if (DL_BITS < 16)
#  define DL_SIZE       LZO_SIZE(DL_BITS)
#  define DL_MASK       LZO_MASK(DL_BITS)
#else
#  define DL_SIZE       LZO_USIZE(DL_BITS)
#  define DL_MASK       LZO_UMASK(DL_BITS)
#endif

#if (D_BITS != DL_BITS + DD_BITS)
#  error "D_BITS does not match"
#endif
#if (D_BITS < 6 || D_BITS > 18)
#  error "invalid D_BITS"
#endif
#if (DL_BITS < 6 || DL_BITS > 20)
#  error "invalid DL_BITS"
#endif
#if (DD_BITS < 0 || DD_BITS > 6)
#  error "invalid DD_BITS"
#endif

#if !defined(DL_MIN_LEN)
#  define DL_MIN_LEN    3
#endif
#if !defined(DL_SHIFT)
#  define DL_SHIFT      ((DL_BITS + (DL_MIN_LEN - 1)) / DL_MIN_LEN)
#endif

#define LZO_HASH_GZIP                   1
#define LZO_HASH_GZIP_INCREMENTAL       2
#define LZO_HASH_LZO_INCREMENTAL_A      3
#define LZO_HASH_LZO_INCREMENTAL_B      4

#if !defined(LZO_HASH)
#  error "choose a hashing strategy"
#endif

#undef DM
#undef DX

#if (DL_MIN_LEN == 3)
#  define _DV2_A(p,shift1,shift2) \
        (((( (lzo_xint)((p)[0]) << shift1) ^ (p)[1]) << shift2) ^ (p)[2])
#  define _DV2_B(p,shift1,shift2) \
        (((( (lzo_xint)((p)[2]) << shift1) ^ (p)[1]) << shift2) ^ (p)[0])
#  define _DV3_B(p,shift1,shift2,shift3) \
        ((_DV2_B((p)+1,shift1,shift2) << (shift3)) ^ (p)[0])
#elif (DL_MIN_LEN == 2)
#  define _DV2_A(p,shift1,shift2) \
        (( (lzo_xint)(p[0]) << shift1) ^ p[1])
#  define _DV2_B(p,shift1,shift2) \
        (( (lzo_xint)(p[1]) << shift1) ^ p[2])
#else
#  error "invalid DL_MIN_LEN"
#endif
#define _DV_A(p,shift)      _DV2_A(p,shift,shift)
#define _DV_B(p,shift)      _DV2_B(p,shift,shift)
#define DA2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) + (p)[1]) << (s1)) + (p)[0])
#define DS2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) - (p)[1]) << (s1)) - (p)[0])
#define DX2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) ^ (p)[1]) << (s1)) ^ (p)[0])
#define DA3(p,s1,s2,s3) ((DA2((p)+1,s2,s3) << (s1)) + (p)[0])
#define DS3(p,s1,s2,s3) ((DS2((p)+1,s2,s3) << (s1)) - (p)[0])
#define DX3(p,s1,s2,s3) ((DX2((p)+1,s2,s3) << (s1)) ^ (p)[0])
#define DMS(v,s)        ((lzo_uint) (((v) & (D_MASK >> (s))) << (s)))
#define DM(v)           DMS(v,0)

#if (LZO_HASH == LZO_HASH_GZIP)
#  define _DINDEX(dv,p)     (_DV_A((p),DL_SHIFT))

#elif (LZO_HASH == LZO_HASH_GZIP_INCREMENTAL)
#  define __LZO_HASH_INCREMENTAL 1
#  define DVAL_FIRST(dv,p)  dv = _DV_A((p),DL_SHIFT)
#  define DVAL_NEXT(dv,p)   dv = (((dv) << DL_SHIFT) ^ p[2])
#  define _DINDEX(dv,p)     (dv)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#elif (LZO_HASH == LZO_HASH_LZO_INCREMENTAL_A)
#  define __LZO_HASH_INCREMENTAL 1
#  define DVAL_FIRST(dv,p)  dv = _DV_A((p),5)
#  define DVAL_NEXT(dv,p) \
                dv ^= (lzo_xint)(p[-1]) << (2*5); dv = (((dv) << 5) ^ p[2])
#  define _DINDEX(dv,p)     ((DMUL(0x9f5f,dv)) >> 5)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#elif (LZO_HASH == LZO_HASH_LZO_INCREMENTAL_B)
#  define __LZO_HASH_INCREMENTAL 1
#  define DVAL_FIRST(dv,p)  dv = _DV_B((p),5)
#  define DVAL_NEXT(dv,p) \
                dv ^= p[-1]; dv = (((dv) >> 5) ^ ((lzo_xint)(p[2]) << (2*5)))
#  define _DINDEX(dv,p)     ((DMUL(0x9f5f,dv)) >> 5)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#else
#  error "choose a hashing strategy"
#endif

#ifndef DINDEX
#define DINDEX(dv,p)        ((lzo_uint)((_DINDEX(dv,p)) & DL_MASK) << DD_BITS)
#endif
#if !defined(DINDEX1) && defined(D_INDEX1)
#define DINDEX1             D_INDEX1
#endif
#if !defined(DINDEX2) && defined(D_INDEX2)
#define DINDEX2             D_INDEX2
#endif

#if !defined(__LZO_HASH_INCREMENTAL)
#  define DVAL_FIRST(dv,p)  ((void) 0)
#  define DVAL_NEXT(dv,p)   ((void) 0)
#  define DVAL_LOOKAHEAD    0
#endif

#if !defined(DVAL_ASSERT)
#if defined(__LZO_HASH_INCREMENTAL) && !defined(NDEBUG)
#if 1 && (LZO_CC_ARMCC_GNUC || LZO_CC_CLANG || (LZO_CC_GNUC >= 0x020700ul) || LZO_CC_INTELC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
static void __attribute__((__unused__))
#else
static void
#endif
DVAL_ASSERT(lzo_xint dv, const lzo_bytep p)
{
    lzo_xint df;
    DVAL_FIRST(df,(p));
    assert(DINDEX(dv,p) == DINDEX(df,p));
}
#else
#  define DVAL_ASSERT(dv,p) ((void) 0)
#endif
#endif

#if (LZO_DICT_USE_PTR)
#  define DENTRY(p,in)                          (p)
#  define GINDEX(m_pos,m_off,dict,dindex,in)    m_pos = dict[dindex]
#else
#  define DENTRY(p,in)                          ((lzo_dict_t) pd(p, in))
#  define GINDEX(m_pos,m_off,dict,dindex,in)    m_off = dict[dindex]
#endif

#if (DD_BITS == 0)

#  define UPDATE_D(dict,drun,dv,p,in)       dict[ DINDEX(dv,p) ] = DENTRY(p,in)
#  define UPDATE_I(dict,drun,index,p,in)    dict[index] = DENTRY(p,in)
#  define UPDATE_P(ptr,drun,p,in)           (ptr)[0] = DENTRY(p,in)

#else

#  define UPDATE_D(dict,drun,dv,p,in)   \
        dict[ DINDEX(dv,p) + drun++ ] = DENTRY(p,in); drun &= DD_MASK
#  define UPDATE_I(dict,drun,index,p,in)    \
        dict[ (index) + drun++ ] = DENTRY(p,in); drun &= DD_MASK
#  define UPDATE_P(ptr,drun,p,in)   \
        (ptr) [ drun++ ] = DENTRY(p,in); drun &= DD_MASK

#endif

#if (LZO_DICT_USE_PTR)

#define LZO_CHECK_MPOS_DET(m_pos,m_off,in,ip,max_offset) \
        (m_pos == NULL || (m_off = pd(ip, m_pos)) > max_offset)

#define LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,max_offset) \
    (BOUNDS_CHECKING_OFF_IN_EXPR(( \
        m_pos = ip - (lzo_uint) PTR_DIFF(ip,m_pos), \
        PTR_LT(m_pos,in) || \
        (m_off = (lzo_uint) PTR_DIFF(ip,m_pos)) == 0 || \
         m_off > max_offset )))

#else

#define LZO_CHECK_MPOS_DET(m_pos,m_off,in,ip,max_offset) \
        (m_off == 0 || \
         ((m_off = pd(ip, in) - m_off) > max_offset) || \
         (m_pos = (ip) - (m_off), 0) )

#define LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,max_offset) \
        (pd(ip, in) <= m_off || \
         ((m_off = pd(ip, in) - m_off) > max_offset) || \
         (m_pos = (ip) - (m_off), 0) )

#endif

#if (LZO_DETERMINISTIC)
#  define LZO_CHECK_MPOS    LZO_CHECK_MPOS_DET
#else
#  define LZO_CHECK_MPOS    LZO_CHECK_MPOS_NON_DET
#endif

#ifdef __cplusplus
}
#endif

#endif

#endif

#endif

#define LZO_DETERMINISTIC !(LZO_DICT_USE_PTR)

#ifndef DO_COMPRESS
#define DO_COMPRESS     lzo1x_1_compress
#endif

#if 1 && defined(DO_COMPRESS) && !defined(do_compress)
#  define do_compress       LZO_PP_ECONCAT2(DO_COMPRESS,_core)
#endif

static __lzo_noinline lzo_uint
do_compress ( const lzo_bytep in , lzo_uint  in_len,
                    lzo_bytep out, lzo_uintp out_len,
                    lzo_uint  ti,  lzo_voidp wrkmem)
{
    const lzo_bytep ip;
    lzo_bytep op;
    const lzo_bytep const in_end = in + in_len;
    const lzo_bytep const ip_end = in + in_len - 20;
    const lzo_bytep ii;
    lzo_dict_p const dict = (lzo_dict_p) wrkmem;

    op = out;
    ip = in;
    ii = ip;

    ip += ti < 4 ? 4 - ti : 0;
    for (;;)
    {
        const lzo_bytep m_pos;
#if !(LZO_DETERMINISTIC)
        LZO_DEFINE_UNINITIALIZED_VAR(lzo_uint, m_off, 0);
        lzo_uint m_len;
        lzo_uint dindex;
next:
        if __lzo_unlikely(ip >= ip_end)
            break;
        DINDEX1(dindex,ip);
        GINDEX(m_pos,m_off,dict,dindex,in);
        if (LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,M4_MAX_OFFSET))
            goto literal;
#if 1
        if (m_off <= M2_MAX_OFFSET || m_pos[3] == ip[3])
            goto try_match;
        DINDEX2(dindex,ip);
#endif
        GINDEX(m_pos,m_off,dict,dindex,in);
        if (LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,M4_MAX_OFFSET))
            goto literal;
        if (m_off <= M2_MAX_OFFSET || m_pos[3] == ip[3])
            goto try_match;
        goto literal;

try_match:
#if (LZO_OPT_UNALIGNED32)
        if (UA_GET_NE32(m_pos) != UA_GET_NE32(ip))
#else
        if (m_pos[0] != ip[0] || m_pos[1] != ip[1] || m_pos[2] != ip[2] || m_pos[3] != ip[3])
#endif
        {
literal:
            UPDATE_I(dict,0,dindex,ip,in);
            ip += 1 + ((ip - ii) >> 5);
            continue;
        }
        UPDATE_I(dict,0,dindex,ip,in);
#else
        lzo_uint m_off;
        lzo_uint m_len;
        {
        lzo_uint32_t dv;
        lzo_uint dindex;
literal:
        ip += 1 + ((ip - ii) >> 5);
next:
        if __lzo_unlikely(ip >= ip_end)
            break;
        dv = UA_GET_LE32(ip);
        dindex = DINDEX(dv,ip);
        GINDEX(m_off,m_pos,in+dict,dindex,in);
        UPDATE_I(dict,0,dindex,ip,in);
        if __lzo_unlikely(dv != UA_GET_LE32(m_pos))
            goto literal;
        }
#endif

        ii -= ti; ti = 0;
        {
        lzo_uint t = pd(ip,ii);
        if (t != 0)
        {
            if (t <= 3)
            {
                op[-2] = LZO_BYTE(op[-2] | t);
#if (LZO_OPT_UNALIGNED32)
                UA_COPY4(op, ii);
                op += t;
#else
                { do *op++ = *ii++; while (--t > 0); }
#endif
            }
#if (LZO_OPT_UNALIGNED32) || (LZO_OPT_UNALIGNED64)
            else if (t <= 16)
            {
                *op++ = LZO_BYTE(t - 3);
                UA_COPY8(op, ii);
                UA_COPY8(op+8, ii+8);
                op += t;
            }
#endif
            else
            {
                if (t <= 18)
                    *op++ = LZO_BYTE(t - 3);
                else
                {
                    lzo_uint tt = t - 18;
                    *op++ = 0;
                    while __lzo_unlikely(tt > 255)
                    {
                        tt -= 255;
                        UA_SET1(op, 0);
                        op++;
                    }
                    assert(tt > 0);
                    *op++ = LZO_BYTE(tt);
                }
#if (LZO_OPT_UNALIGNED32) || (LZO_OPT_UNALIGNED64)
                do {
                    UA_COPY8(op, ii);
                    UA_COPY8(op+8, ii+8);
                    op += 16; ii += 16; t -= 16;
                } while (t >= 16); if (t > 0)
#endif
                { do *op++ = *ii++; while (--t > 0); }
            }
        }
        }
        m_len = 4;
        {
#if (LZO_OPT_UNALIGNED64)
        lzo_uint64_t v;
        v = UA_GET_NE64(ip + m_len) ^ UA_GET_NE64(m_pos + m_len);
        if __lzo_unlikely(v == 0) {
            do {
                m_len += 8;
                v = UA_GET_NE64(ip + m_len) ^ UA_GET_NE64(m_pos + m_len);
                if __lzo_unlikely(ip + m_len >= ip_end)
                    goto m_len_done;
            } while (v == 0);
        }
#if (LZO_ABI_BIG_ENDIAN) && defined(lzo_bitops_ctlz64)
        m_len += lzo_bitops_ctlz64(v) / CHAR_BIT;
#elif (LZO_ABI_BIG_ENDIAN)
        if ((v >> (64 - CHAR_BIT)) == 0) do {
            v <<= CHAR_BIT;
            m_len += 1;
        } while ((v >> (64 - CHAR_BIT)) == 0);
#elif (LZO_ABI_LITTLE_ENDIAN) && defined(lzo_bitops_cttz64)
        m_len += lzo_bitops_cttz64(v) / CHAR_BIT;
#elif (LZO_ABI_LITTLE_ENDIAN)
        if ((v & UCHAR_MAX) == 0) do {
            v >>= CHAR_BIT;
            m_len += 1;
        } while ((v & UCHAR_MAX) == 0);
#else
        if (ip[m_len] == m_pos[m_len]) do {
            m_len += 1;
        } while (ip[m_len] == m_pos[m_len]);
#endif
#elif (LZO_OPT_UNALIGNED32)
        lzo_uint32_t v;
        v = UA_GET_NE32(ip + m_len) ^ UA_GET_NE32(m_pos + m_len);
        if __lzo_unlikely(v == 0) {
            do {
                m_len += 4;
                v = UA_GET_NE32(ip + m_len) ^ UA_GET_NE32(m_pos + m_len);
                if (v != 0)
                    break;
                m_len += 4;
                v = UA_GET_NE32(ip + m_len) ^ UA_GET_NE32(m_pos + m_len);
                if __lzo_unlikely(ip + m_len >= ip_end)
                    goto m_len_done;
            } while (v == 0);
        }
#if (LZO_ABI_BIG_ENDIAN) && defined(lzo_bitops_ctlz32)
        m_len += lzo_bitops_ctlz32(v) / CHAR_BIT;
#elif (LZO_ABI_BIG_ENDIAN)
        if ((v >> (32 - CHAR_BIT)) == 0) do {
            v <<= CHAR_BIT;
            m_len += 1;
        } while ((v >> (32 - CHAR_BIT)) == 0);
#elif (LZO_ABI_LITTLE_ENDIAN) && defined(lzo_bitops_cttz32)
        m_len += lzo_bitops_cttz32(v) / CHAR_BIT;
#elif (LZO_ABI_LITTLE_ENDIAN)
        if ((v & UCHAR_MAX) == 0) do {
            v >>= CHAR_BIT;
            m_len += 1;
        } while ((v & UCHAR_MAX) == 0);
#else
        if (ip[m_len] == m_pos[m_len]) do {
            m_len += 1;
        } while (ip[m_len] == m_pos[m_len]);
#endif
#else
        if __lzo_unlikely(ip[m_len] == m_pos[m_len]) {
            do {
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if (ip[m_len] != m_pos[m_len])
                    break;
                m_len += 1;
                if __lzo_unlikely(ip + m_len >= ip_end)
                    goto m_len_done;
            } while (ip[m_len] == m_pos[m_len]);
        }
#endif
        }
m_len_done:
        m_off = pd(ip,m_pos);
        ip += m_len;
        ii = ip;
        if (m_len <= M2_MAX_LEN && m_off <= M2_MAX_OFFSET)
        {
            m_off -= 1;
#if defined(LZO1X)
            *op++ = LZO_BYTE(((m_len - 1) << 5) | ((m_off & 7) << 2));
            *op++ = LZO_BYTE(m_off >> 3);
#elif defined(LZO1Y)
            *op++ = LZO_BYTE(((m_len + 1) << 4) | ((m_off & 3) << 2));
            *op++ = LZO_BYTE(m_off >> 2);
#endif
        }
        else if (m_off <= M3_MAX_OFFSET)
        {
            m_off -= 1;
            if (m_len <= M3_MAX_LEN)
                *op++ = LZO_BYTE(M3_MARKER | (m_len - 2));
            else
            {
                m_len -= M3_MAX_LEN;
                *op++ = M3_MARKER | 0;
                while __lzo_unlikely(m_len > 255)
                {
                    m_len -= 255;
                    UA_SET1(op, 0);
                    op++;
                }
                *op++ = LZO_BYTE(m_len);
            }
            *op++ = LZO_BYTE(m_off << 2);
            *op++ = LZO_BYTE(m_off >> 6);
        }
        else
        {
            m_off -= 0x4000;
            if (m_len <= M4_MAX_LEN)
                *op++ = LZO_BYTE(M4_MARKER | ((m_off >> 11) & 8) | (m_len - 2));
            else
            {
                m_len -= M4_MAX_LEN;
                *op++ = LZO_BYTE(M4_MARKER | ((m_off >> 11) & 8));
                while __lzo_unlikely(m_len > 255)
                {
                    m_len -= 255;
                    UA_SET1(op, 0);
                    op++;
                }
                *op++ = LZO_BYTE(m_len);
            }
            *op++ = LZO_BYTE(m_off << 2);
            *op++ = LZO_BYTE(m_off >> 6);
        }
        goto next;
    }

    *out_len = pd(op, out);
    return pd(in_end,ii-ti);
}

LZO_PUBLIC(int)
DO_COMPRESS      ( const lzo_bytep in , lzo_uint  in_len,
                         lzo_bytep out, lzo_uintp out_len,
                         lzo_voidp wrkmem )
{
    const lzo_bytep ip = in;
    lzo_bytep op = out;
    lzo_uint l = in_len;
    lzo_uint t = 0;

    while (l > 20)
    {
        lzo_uint ll = l;
        lzo_uintptr_t ll_end;
#if 0 || (LZO_DETERMINISTIC)
        ll = LZO_MIN(ll, 49152);
#endif
        ll_end = (lzo_uintptr_t)ip + ll;
        if ((ll_end + ((t + ll) >> 5)) <= ll_end || (const lzo_bytep)(ll_end + ((t + ll) >> 5)) <= ip + ll)
            break;
#if (LZO_DETERMINISTIC)
        lzo_memset(wrkmem, 0, ((lzo_uint)1 << D_BITS) * sizeof(lzo_dict_t));
#endif
        t = do_compress(ip,ll,op,out_len,t,wrkmem);
        ip += ll;
        op += *out_len;
        l  -= ll;
    }
    t += l;

    if (t > 0)
    {
        const lzo_bytep ii = in + in_len - t;

        if (op == out && t <= 238)
            *op++ = LZO_BYTE(17 + t);
        else if (t <= 3)
            op[-2] = LZO_BYTE(op[-2] | t);
        else if (t <= 18)
            *op++ = LZO_BYTE(t - 3);
        else
        {
            lzo_uint tt = t - 18;

            *op++ = 0;
            while (tt > 255)
            {
                tt -= 255;
                UA_SET1(op, 0);
                op++;
            }
            assert(tt > 0);
            *op++ = LZO_BYTE(tt);
        }
        UA_COPYN(op, ii, t);
        op += t;
    }

    *op++ = M4_MARKER | 1;
    *op++ = 0;
    *op++ = 0;

    *out_len = pd(op, out);
    return LZO_E_OK;
}

#endif

#undef do_compress
#undef DO_COMPRESS
#undef LZO_HASH

#undef LZO_TEST_OVERRUN
#undef DO_DECOMPRESS
#define DO_DECOMPRESS       lzo1x_decompress

#if !defined(MINILZO_CFG_SKIP_LZO1X_DECOMPRESS)

#if defined(LZO_TEST_OVERRUN)
#  if !defined(LZO_TEST_OVERRUN_INPUT)
#    define LZO_TEST_OVERRUN_INPUT       2
#  endif
#  if !defined(LZO_TEST_OVERRUN_OUTPUT)
#    define LZO_TEST_OVERRUN_OUTPUT      2
#  endif
#  if !defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#    define LZO_TEST_OVERRUN_LOOKBEHIND  1
#  endif
#endif

#undef TEST_IP
#undef TEST_OP
#undef TEST_IP_AND_TEST_OP
#undef TEST_LB
#undef TEST_LBO
#undef NEED_IP
#undef NEED_OP
#undef TEST_IV
#undef TEST_OV
#undef HAVE_TEST_IP
#undef HAVE_TEST_OP
#undef HAVE_NEED_IP
#undef HAVE_NEED_OP
#undef HAVE_ANY_IP
#undef HAVE_ANY_OP

#if defined(LZO_TEST_OVERRUN_INPUT)
#  if (LZO_TEST_OVERRUN_INPUT >= 1)
#    define TEST_IP             (ip < ip_end)
#  endif
#  if (LZO_TEST_OVERRUN_INPUT >= 2)
#    define NEED_IP(x) \
            if ((lzo_uint)(ip_end - ip) < (lzo_uint)(x))  goto input_overrun
#    define TEST_IV(x)          if ((x) >  (lzo_uint)0 - (511)) goto input_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_OUTPUT)
#  if (LZO_TEST_OVERRUN_OUTPUT >= 1)
#    define TEST_OP             (op <= op_end)
#  endif
#  if (LZO_TEST_OVERRUN_OUTPUT >= 2)
#    undef TEST_OP
#    define NEED_OP(x) \
            if ((lzo_uint)(op_end - op) < (lzo_uint)(x))  goto output_overrun
#    define TEST_OV(x)          if ((x) >  (lzo_uint)0 - (511)) goto output_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#  define TEST_LB(m_pos)        if (PTR_LT(m_pos,out) || PTR_GE(m_pos,op)) goto lookbehind_overrun
#  define TEST_LBO(m_pos,o)     if (PTR_LT(m_pos,out) || PTR_GE(m_pos,op-(o))) goto lookbehind_overrun
#else
#  define TEST_LB(m_pos)        ((void) 0)
#  define TEST_LBO(m_pos,o)     ((void) 0)
#endif

#if !defined(LZO_EOF_CODE) && !defined(TEST_IP)
#  define TEST_IP               (ip < ip_end)
#endif

#if defined(TEST_IP)
#  define HAVE_TEST_IP 1
#else
#  define TEST_IP               1
#endif
#if defined(TEST_OP)
#  define HAVE_TEST_OP 1
#else
#  define TEST_OP               1
#endif

#if defined(HAVE_TEST_IP) && defined(HAVE_TEST_OP)
#  define TEST_IP_AND_TEST_OP   (TEST_IP && TEST_OP)
#elif defined(HAVE_TEST_IP)
#  define TEST_IP_AND_TEST_OP   TEST_IP
#elif defined(HAVE_TEST_OP)
#  define TEST_IP_AND_TEST_OP   TEST_OP
#else
#  define TEST_IP_AND_TEST_OP   1
#endif

#if defined(NEED_IP)
#  define HAVE_NEED_IP 1
#else
#  define NEED_IP(x)            ((void) 0)
#  define TEST_IV(x)            ((void) 0)
#endif
#if defined(NEED_OP)
#  define HAVE_NEED_OP 1
#else
#  define NEED_OP(x)            ((void) 0)
#  define TEST_OV(x)            ((void) 0)
#endif

#if defined(HAVE_TEST_IP) || defined(HAVE_NEED_IP)
#  define HAVE_ANY_IP 1
#endif
#if defined(HAVE_TEST_OP) || defined(HAVE_NEED_OP)
#  define HAVE_ANY_OP 1
#endif

#if defined(DO_DECOMPRESS)
LZO_PUBLIC(int)
DO_DECOMPRESS  ( const lzo_bytep in , lzo_uint  in_len,
                       lzo_bytep out, lzo_uintp out_len,
                       lzo_voidp wrkmem )
#endif
{
    lzo_bytep op;
    const lzo_bytep ip;
    lzo_uint t;
#if defined(COPY_DICT)
    lzo_uint m_off;
    const lzo_bytep dict_end;
#else
    const lzo_bytep m_pos;
#endif

    const lzo_bytep const ip_end = in + in_len;
#if defined(HAVE_ANY_OP)
    lzo_bytep const op_end = out + *out_len;
#endif
#if defined(LZO1Z)
    lzo_uint last_m_off = 0;
#endif

    LZO_UNUSED(wrkmem);

#if defined(COPY_DICT)
    if (dict)
    {
        if (dict_len > M4_MAX_OFFSET)
        {
            dict += dict_len - M4_MAX_OFFSET;
            dict_len = M4_MAX_OFFSET;
        }
        dict_end = dict + dict_len;
    }
    else
    {
        dict_len = 0;
        dict_end = NULL;
    }
#endif

    *out_len = 0;

    op = out;
    ip = in;

    NEED_IP(1);
    if (*ip > 17)
    {
        t = *ip++ - 17;
        if (t < 4)
            goto match_next;
        assert(t > 0); NEED_OP(t); NEED_IP(t+3);
        do *op++ = *ip++; while (--t > 0);
        goto first_literal_run;
    }

    for (;;)
    {
        NEED_IP(3);
        t = *ip++;
        if (t >= 16)
            goto match;
        if (t == 0)
        {
            while (*ip == 0)
            {
                t += 255;
                ip++;
                TEST_IV(t);
                NEED_IP(1);
            }
            t += 15 + *ip++;
        }
        assert(t > 0); NEED_OP(t+3); NEED_IP(t+6);
#if (LZO_OPT_UNALIGNED64) && (LZO_OPT_UNALIGNED32)
        t += 3;
        if (t >= 8) do
        {
            UA_COPY8(op,ip);
            op += 8; ip += 8; t -= 8;
        } while (t >= 8);
        if (t >= 4)
        {
            UA_COPY4(op,ip);
            op += 4; ip += 4; t -= 4;
        }
        if (t > 0)
        {
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
        }
#elif (LZO_OPT_UNALIGNED32) || (LZO_ALIGNED_OK_4)
#if !(LZO_OPT_UNALIGNED32)
        if (PTR_ALIGNED2_4(op,ip))
        {
#endif
        UA_COPY4(op,ip);
        op += 4; ip += 4;
        if (--t > 0)
        {
            if (t >= 4)
            {
                do {
                    UA_COPY4(op,ip);
                    op += 4; ip += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *ip++; while (--t > 0);
            }
            else
                do *op++ = *ip++; while (--t > 0);
        }
#if !(LZO_OPT_UNALIGNED32)
        }
        else
#endif
#endif
#if !(LZO_OPT_UNALIGNED32)
        {
            *op++ = *ip++; *op++ = *ip++; *op++ = *ip++;
            do *op++ = *ip++; while (--t > 0);
        }
#endif

first_literal_run:

        t = *ip++;
        if (t >= 16)
            goto match;
#if defined(COPY_DICT)
#if defined(LZO1Z)
        m_off = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        last_m_off = m_off;
#else
        m_off = (1 + M2_MAX_OFFSET) + (t >> 2) + (*ip++ << 2);
#endif
        NEED_OP(3);
        t = 3; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
        t = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        m_pos = op - t;
        last_m_off = t;
#else
        m_pos = op - (1 + M2_MAX_OFFSET);
        m_pos -= t >> 2;
        m_pos -= *ip++ << 2;
#endif
        TEST_LB(m_pos); NEED_OP(3);
        *op++ = *m_pos++; *op++ = *m_pos++; *op++ = *m_pos;
#endif
        goto match_done;

        for (;;) {
match:
            if (t >= 64)
            {
#if defined(COPY_DICT)
#if defined(LZO1X)
                m_off = 1 + ((t >> 2) & 7) + (*ip++ << 3);
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_off = 1 + ((t >> 2) & 3) + (*ip++ << 2);
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                m_off = t & 0x1f;
                if (m_off >= 0x1c)
                    m_off = last_m_off;
                else
                {
                    m_off = 1 + (m_off << 6) + (*ip++ >> 2);
                    last_m_off = m_off;
                }
                t = (t >> 5) - 1;
#endif
#else
#if defined(LZO1X)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 7;
                m_pos -= *ip++ << 3;
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 3;
                m_pos -= *ip++ << 2;
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                {
                    lzo_uint off = t & 0x1f;
                    m_pos = op;
                    if (off >= 0x1c)
                    {
                        assert(last_m_off > 0);
                        m_pos -= last_m_off;
                    }
                    else
                    {
                        off = 1 + (off << 6) + (*ip++ >> 2);
                        m_pos -= off;
                        last_m_off = off;
                    }
                }
                t = (t >> 5) - 1;
#endif
                TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
                goto copy_match;
#endif
            }
            else if (t >= 32)
            {
                t &= 31;
                if (t == 0)
                {
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        TEST_OV(t);
                        NEED_IP(1);
                    }
                    t += 31 + *ip++;
                    NEED_IP(2);
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (ip[0] >> 2) + (ip[1] << 6);
#endif
#else
#if defined(LZO1Z)
                {
                    lzo_uint off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                    m_pos = op - off;
                    last_m_off = off;
                }
#elif (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
                m_pos = op - 1;
                m_pos -= UA_GET_LE16(ip) >> 2;
#else
                m_pos = op - 1;
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
#endif
                ip += 2;
            }
            else if (t >= 16)
            {
#if defined(COPY_DICT)
                m_off = (t & 8) << 11;
#else
                m_pos = op;
                m_pos -= (t & 8) << 11;
#endif
                t &= 7;
                if (t == 0)
                {
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        TEST_OV(t);
                        NEED_IP(1);
                    }
                    t += 7 + *ip++;
                    NEED_IP(2);
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off += (ip[0] << 6) + (ip[1] >> 2);
#else
                m_off += (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_off == 0)
                    goto eof_found;
                m_off += 0x4000;
#if defined(LZO1Z)
                last_m_off = m_off;
#endif
#else
#if defined(LZO1Z)
                m_pos -= (ip[0] << 6) + (ip[1] >> 2);
#elif (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
                m_pos -= UA_GET_LE16(ip) >> 2;
#else
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_pos == op)
                    goto eof_found;
                m_pos -= 0x4000;
#if defined(LZO1Z)
                last_m_off = pd((const lzo_bytep)op, m_pos);
#endif
#endif
            }
            else
            {
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (t << 6) + (*ip++ >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (t >> 2) + (*ip++ << 2);
#endif
                NEED_OP(2);
                t = 2; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
                t = 1 + (t << 6) + (*ip++ >> 2);
                m_pos = op - t;
                last_m_off = t;
#else
                m_pos = op - 1;
                m_pos -= t >> 2;
                m_pos -= *ip++ << 2;
#endif
                TEST_LB(m_pos); NEED_OP(2);
                *op++ = *m_pos++; *op++ = *m_pos;
#endif
                goto match_done;
            }

#if defined(COPY_DICT)

            NEED_OP(t+3-1);
            t += 3-1; COPY_DICT(t,m_off)

#else

            TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
#if (LZO_OPT_UNALIGNED64) && (LZO_OPT_UNALIGNED32)
            if (op - m_pos >= 8)
            {
                t += (3 - 1);
                if (t >= 8) do
                {
                    UA_COPY8(op,m_pos);
                    op += 8; m_pos += 8; t -= 8;
                } while (t >= 8);
                if (t >= 4)
                {
                    UA_COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                }
                if (t > 0)
                {
                    *op++ = m_pos[0];
                    if (t > 1) { *op++ = m_pos[1]; if (t > 2) { *op++ = m_pos[2]; } }
                }
            }
            else
#elif (LZO_OPT_UNALIGNED32) || (LZO_ALIGNED_OK_4)
#if !(LZO_OPT_UNALIGNED32)
            if (t >= 2 * 4 - (3 - 1) && PTR_ALIGNED2_4(op,m_pos))
            {
                assert((op - m_pos) >= 4);
#else
            if (t >= 2 * 4 - (3 - 1) && (op - m_pos) >= 4)
            {
#endif
                UA_COPY4(op,m_pos);
                op += 4; m_pos += 4; t -= 4 - (3 - 1);
                do {
                    UA_COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *m_pos++; while (--t > 0);
            }
            else
#endif
            {
copy_match:
                *op++ = *m_pos++; *op++ = *m_pos++;
                do *op++ = *m_pos++; while (--t > 0);
            }

#endif

match_done:
#if defined(LZO1Z)
            t = ip[-1] & 3;
#else
            t = ip[-2] & 3;
#endif
            if (t == 0)
                break;

match_next:
            assert(t > 0); assert(t < 4); NEED_OP(t); NEED_IP(t+3);
#if 0
            do *op++ = *ip++; while (--t > 0);
#else
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
#endif
            t = *ip++;
        }
    }

eof_found:
    *out_len = pd(op, out);
    return (ip == ip_end ? LZO_E_OK :
           (ip < ip_end  ? LZO_E_INPUT_NOT_CONSUMED : LZO_E_INPUT_OVERRUN));

#if defined(HAVE_NEED_IP)
input_overrun:
    *out_len = pd(op, out);
    return LZO_E_INPUT_OVERRUN;
#endif

#if defined(HAVE_NEED_OP)
output_overrun:
    *out_len = pd(op, out);
    return LZO_E_OUTPUT_OVERRUN;
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
lookbehind_overrun:
    *out_len = pd(op, out);
    return LZO_E_LOOKBEHIND_OVERRUN;
#endif
}

#endif

#define LZO_TEST_OVERRUN 1
#undef DO_DECOMPRESS
#define DO_DECOMPRESS       lzo1x_decompress_safe

#if !defined(MINILZO_CFG_SKIP_LZO1X_DECOMPRESS_SAFE)

#if defined(LZO_TEST_OVERRUN)
#  if !defined(LZO_TEST_OVERRUN_INPUT)
#    define LZO_TEST_OVERRUN_INPUT       2
#  endif
#  if !defined(LZO_TEST_OVERRUN_OUTPUT)
#    define LZO_TEST_OVERRUN_OUTPUT      2
#  endif
#  if !defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#    define LZO_TEST_OVERRUN_LOOKBEHIND  1
#  endif
#endif

#undef TEST_IP
#undef TEST_OP
#undef TEST_IP_AND_TEST_OP
#undef TEST_LB
#undef TEST_LBO
#undef NEED_IP
#undef NEED_OP
#undef TEST_IV
#undef TEST_OV
#undef HAVE_TEST_IP
#undef HAVE_TEST_OP
#undef HAVE_NEED_IP
#undef HAVE_NEED_OP
#undef HAVE_ANY_IP
#undef HAVE_ANY_OP

#if defined(LZO_TEST_OVERRUN_INPUT)
#  if (LZO_TEST_OVERRUN_INPUT >= 1)
#    define TEST_IP             (ip < ip_end)
#  endif
#  if (LZO_TEST_OVERRUN_INPUT >= 2)
#    define NEED_IP(x) \
            if ((lzo_uint)(ip_end - ip) < (lzo_uint)(x))  goto input_overrun
#    define TEST_IV(x)          if ((x) >  (lzo_uint)0 - (511)) goto input_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_OUTPUT)
#  if (LZO_TEST_OVERRUN_OUTPUT >= 1)
#    define TEST_OP             (op <= op_end)
#  endif
#  if (LZO_TEST_OVERRUN_OUTPUT >= 2)
#    undef TEST_OP
#    define NEED_OP(x) \
            if ((lzo_uint)(op_end - op) < (lzo_uint)(x))  goto output_overrun
#    define TEST_OV(x)          if ((x) >  (lzo_uint)0 - (511)) goto output_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#  define TEST_LB(m_pos)        if (PTR_LT(m_pos,out) || PTR_GE(m_pos,op)) goto lookbehind_overrun
#  define TEST_LBO(m_pos,o)     if (PTR_LT(m_pos,out) || PTR_GE(m_pos,op-(o))) goto lookbehind_overrun
#else
#  define TEST_LB(m_pos)        ((void) 0)
#  define TEST_LBO(m_pos,o)     ((void) 0)
#endif

#if !defined(LZO_EOF_CODE) && !defined(TEST_IP)
#  define TEST_IP               (ip < ip_end)
#endif

#if defined(TEST_IP)
#  define HAVE_TEST_IP 1
#else
#  define TEST_IP               1
#endif
#if defined(TEST_OP)
#  define HAVE_TEST_OP 1
#else
#  define TEST_OP               1
#endif

#if defined(HAVE_TEST_IP) && defined(HAVE_TEST_OP)
#  define TEST_IP_AND_TEST_OP   (TEST_IP && TEST_OP)
#elif defined(HAVE_TEST_IP)
#  define TEST_IP_AND_TEST_OP   TEST_IP
#elif defined(HAVE_TEST_OP)
#  define TEST_IP_AND_TEST_OP   TEST_OP
#else
#  define TEST_IP_AND_TEST_OP   1
#endif

#if defined(NEED_IP)
#  define HAVE_NEED_IP 1
#else
#  define NEED_IP(x)            ((void) 0)
#  define TEST_IV(x)            ((void) 0)
#endif
#if defined(NEED_OP)
#  define HAVE_NEED_OP 1
#else
#  define NEED_OP(x)            ((void) 0)
#  define TEST_OV(x)            ((void) 0)
#endif

#if defined(HAVE_TEST_IP) || defined(HAVE_NEED_IP)
#  define HAVE_ANY_IP 1
#endif
#if defined(HAVE_TEST_OP) || defined(HAVE_NEED_OP)
#  define HAVE_ANY_OP 1
#endif

#if defined(DO_DECOMPRESS)
LZO_PUBLIC(int)
DO_DECOMPRESS  ( const lzo_bytep in , lzo_uint  in_len,
                       lzo_bytep out, lzo_uintp out_len,
                       lzo_voidp wrkmem )
#endif
{
    lzo_bytep op;
    const lzo_bytep ip;
    lzo_uint t;
#if defined(COPY_DICT)
    lzo_uint m_off;
    const lzo_bytep dict_end;
#else
    const lzo_bytep m_pos;
#endif

    const lzo_bytep const ip_end = in + in_len;
#if defined(HAVE_ANY_OP)
    lzo_bytep const op_end = out + *out_len;
#endif
#if defined(LZO1Z)
    lzo_uint last_m_off = 0;
#endif

    LZO_UNUSED(wrkmem);

#if defined(COPY_DICT)
    if (dict)
    {
        if (dict_len > M4_MAX_OFFSET)
        {
            dict += dict_len - M4_MAX_OFFSET;
            dict_len = M4_MAX_OFFSET;
        }
        dict_end = dict + dict_len;
    }
    else
    {
        dict_len = 0;
        dict_end = NULL;
    }
#endif

    *out_len = 0;

    op = out;
    ip = in;

    NEED_IP(1);
    if (*ip > 17)
    {
        t = *ip++ - 17;
        if (t < 4)
            goto match_next;
        assert(t > 0); NEED_OP(t); NEED_IP(t+3);
        do *op++ = *ip++; while (--t > 0);
        goto first_literal_run;
    }

    for (;;)
    {
        NEED_IP(3);
        t = *ip++;
        if (t >= 16)
            goto match;
        if (t == 0)
        {
            while (*ip == 0)
            {
                t += 255;
                ip++;
                TEST_IV(t);
                NEED_IP(1);
            }
            t += 15 + *ip++;
        }
        assert(t > 0); NEED_OP(t+3); NEED_IP(t+6);
#if (LZO_OPT_UNALIGNED64) && (LZO_OPT_UNALIGNED32)
        t += 3;
        if (t >= 8) do
        {
            UA_COPY8(op,ip);
            op += 8; ip += 8; t -= 8;
        } while (t >= 8);
        if (t >= 4)
        {
            UA_COPY4(op,ip);
            op += 4; ip += 4; t -= 4;
        }
        if (t > 0)
        {
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
        }
#elif (LZO_OPT_UNALIGNED32) || (LZO_ALIGNED_OK_4)
#if !(LZO_OPT_UNALIGNED32)
        if (PTR_ALIGNED2_4(op,ip))
        {
#endif
        UA_COPY4(op,ip);
        op += 4; ip += 4;
        if (--t > 0)
        {
            if (t >= 4)
            {
                do {
                    UA_COPY4(op,ip);
                    op += 4; ip += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *ip++; while (--t > 0);
            }
            else
                do *op++ = *ip++; while (--t > 0);
        }
#if !(LZO_OPT_UNALIGNED32)
        }
        else
#endif
#endif
#if !(LZO_OPT_UNALIGNED32)
        {
            *op++ = *ip++; *op++ = *ip++; *op++ = *ip++;
            do *op++ = *ip++; while (--t > 0);
        }
#endif

first_literal_run:

        t = *ip++;
        if (t >= 16)
            goto match;
#if defined(COPY_DICT)
#if defined(LZO1Z)
        m_off = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        last_m_off = m_off;
#else
        m_off = (1 + M2_MAX_OFFSET) + (t >> 2) + (*ip++ << 2);
#endif
        NEED_OP(3);
        t = 3; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
        t = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        m_pos = op - t;
        last_m_off = t;
#else
        m_pos = op - (1 + M2_MAX_OFFSET);
        m_pos -= t >> 2;
        m_pos -= *ip++ << 2;
#endif
        TEST_LB(m_pos); NEED_OP(3);
        *op++ = *m_pos++; *op++ = *m_pos++; *op++ = *m_pos;
#endif
        goto match_done;

        for (;;) {
match:
            if (t >= 64)
            {
#if defined(COPY_DICT)
#if defined(LZO1X)
                m_off = 1 + ((t >> 2) & 7) + (*ip++ << 3);
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_off = 1 + ((t >> 2) & 3) + (*ip++ << 2);
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                m_off = t & 0x1f;
                if (m_off >= 0x1c)
                    m_off = last_m_off;
                else
                {
                    m_off = 1 + (m_off << 6) + (*ip++ >> 2);
                    last_m_off = m_off;
                }
                t = (t >> 5) - 1;
#endif
#else
#if defined(LZO1X)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 7;
                m_pos -= *ip++ << 3;
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 3;
                m_pos -= *ip++ << 2;
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                {
                    lzo_uint off = t & 0x1f;
                    m_pos = op;
                    if (off >= 0x1c)
                    {
                        assert(last_m_off > 0);
                        m_pos -= last_m_off;
                    }
                    else
                    {
                        off = 1 + (off << 6) + (*ip++ >> 2);
                        m_pos -= off;
                        last_m_off = off;
                    }
                }
                t = (t >> 5) - 1;
#endif
                TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
                goto copy_match;
#endif
            }
            else if (t >= 32)
            {
                t &= 31;
                if (t == 0)
                {
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        TEST_OV(t);
                        NEED_IP(1);
                    }
                    t += 31 + *ip++;
                    NEED_IP(2);
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (ip[0] >> 2) + (ip[1] << 6);
#endif
#else
#if defined(LZO1Z)
                {
                    lzo_uint off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                    m_pos = op - off;
                    last_m_off = off;
                }
#elif (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
                m_pos = op - 1;
                m_pos -= UA_GET_LE16(ip) >> 2;
#else
                m_pos = op - 1;
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
#endif
                ip += 2;
            }
            else if (t >= 16)
            {
#if defined(COPY_DICT)
                m_off = (t & 8) << 11;
#else
                m_pos = op;
                m_pos -= (t & 8) << 11;
#endif
                t &= 7;
                if (t == 0)
                {
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        TEST_OV(t);
                        NEED_IP(1);
                    }
                    t += 7 + *ip++;
                    NEED_IP(2);
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off += (ip[0] << 6) + (ip[1] >> 2);
#else
                m_off += (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_off == 0)
                    goto eof_found;
                m_off += 0x4000;
#if defined(LZO1Z)
                last_m_off = m_off;
#endif
#else
#if defined(LZO1Z)
                m_pos -= (ip[0] << 6) + (ip[1] >> 2);
#elif (LZO_OPT_UNALIGNED16) && (LZO_ABI_LITTLE_ENDIAN)
                m_pos -= UA_GET_LE16(ip) >> 2;
#else
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_pos == op)
                    goto eof_found;
                m_pos -= 0x4000;
#if defined(LZO1Z)
                last_m_off = pd((const lzo_bytep)op, m_pos);
#endif
#endif
            }
            else
            {
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (t << 6) + (*ip++ >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (t >> 2) + (*ip++ << 2);
#endif
                NEED_OP(2);
                t = 2; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
                t = 1 + (t << 6) + (*ip++ >> 2);
                m_pos = op - t;
                last_m_off = t;
#else
                m_pos = op - 1;
                m_pos -= t >> 2;
                m_pos -= *ip++ << 2;
#endif
                TEST_LB(m_pos); NEED_OP(2);
                *op++ = *m_pos++; *op++ = *m_pos;
#endif
                goto match_done;
            }

#if defined(COPY_DICT)

            NEED_OP(t+3-1);
            t += 3-1; COPY_DICT(t,m_off)

#else

            TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
#if (LZO_OPT_UNALIGNED64) && (LZO_OPT_UNALIGNED32)
            if (op - m_pos >= 8)
            {
                t += (3 - 1);
                if (t >= 8) do
                {
                    UA_COPY8(op,m_pos);
                    op += 8; m_pos += 8; t -= 8;
                } while (t >= 8);
                if (t >= 4)
                {
                    UA_COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                }
                if (t > 0)
                {
                    *op++ = m_pos[0];
                    if (t > 1) { *op++ = m_pos[1]; if (t > 2) { *op++ = m_pos[2]; } }
                }
            }
            else
#elif (LZO_OPT_UNALIGNED32) || (LZO_ALIGNED_OK_4)
#if !(LZO_OPT_UNALIGNED32)
            if (t >= 2 * 4 - (3 - 1) && PTR_ALIGNED2_4(op,m_pos))
            {
                assert((op - m_pos) >= 4);
#else
            if (t >= 2 * 4 - (3 - 1) && (op - m_pos) >= 4)
            {
#endif
                UA_COPY4(op,m_pos);
                op += 4; m_pos += 4; t -= 4 - (3 - 1);
                do {
                    UA_COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *m_pos++; while (--t > 0);
            }
            else
#endif
            {
copy_match:
                *op++ = *m_pos++; *op++ = *m_pos++;
                do *op++ = *m_pos++; while (--t > 0);
            }

#endif

match_done:
#if defined(LZO1Z)
            t = ip[-1] & 3;
#else
            t = ip[-2] & 3;
#endif
            if (t == 0)
                break;

match_next:
            assert(t > 0); assert(t < 4); NEED_OP(t); NEED_IP(t+3);
#if 0
            do *op++ = *ip++; while (--t > 0);
#else
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
#endif
            t = *ip++;
        }
    }

eof_found:
    *out_len = pd(op, out);
    return (ip == ip_end ? LZO_E_OK :
           (ip < ip_end  ? LZO_E_INPUT_NOT_CONSUMED : LZO_E_INPUT_OVERRUN));

#if defined(HAVE_NEED_IP)
input_overrun:
    *out_len = pd(op, out);
    return LZO_E_INPUT_OVERRUN;
#endif

#if defined(HAVE_NEED_OP)
output_overrun:
    *out_len = pd(op, out);
    return LZO_E_OUTPUT_OVERRUN;
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
lookbehind_overrun:
    *out_len = pd(op, out);
    return LZO_E_LOOKBEHIND_OVERRUN;
#endif
}

#endif

/***** End of minilzo.c *****/
