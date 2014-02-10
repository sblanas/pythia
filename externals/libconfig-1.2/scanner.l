/* -*- mode: C -*- */
/* --------------------------------------------------------------------------
   libconfig - A structured configuration file parsing library
   Copyright (C) 2005-2007  Mark A Lindner
 
   This file is part of libconfig.
    
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public License
   as published by the Free Software Foundation; either version 2.1 of
   the License, or (at your option) any later version.
    
   This library is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
    
   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
   ----------------------------------------------------------------------------
*/

%{
#define YY_EXTRA_TYPE void*
%}

%option nounistd
%option reentrant
%option noyywrap
%option yylineno
%option nounput
%option bison-bridge
%option header-file="scanner.h"
%option outfile="lex.yy.c"

%{

#ifdef _MSC_VER
#pragma warning (disable: 4996)
#endif

#include <stdlib.h>
#include <string.h>
#include "grammar.h"

/* this is somewhat kludgy, but I wanted to avoid building strings
  dynamically during scanning */

static char *make_string(char *s)
{
  char *r = ++s;
  char *p, *q = r;
  size_t len = strlen(r);
  int esc = 0;

  *(r + --len) = 0;

  for(p = r; *p; p++)
  {
    if(*p == '\\')
    {
      if(! esc)
      {
        esc = 1;
	continue;
      }
    }
     
    if(esc)
    {
      if(*p == 'n')
        *(q++) = '\n';
      else if(*p == 'r')
        *(q++) = '\r';
      else if(*p == 'f')
        *(q++) = '\f';
      else if(*p == 't')
        *(q++) = '\t';
      else
        *(q++) = *p;

      esc = 0;
    }
    
    else if(*p == '\"') /* if we reached the end of a string segment, ... */
    {
       /* This construction allows for C-style string concatenation.
          We don't bother to check for end-of-string here, as we depend
 	  on the {string} definition to ensure a new opening quote exists.
 	  We do, however, check for and discard all forms of comments
 	  [that is, (#...$|//...$|[/][*]...[*][/])] between string segments. */

      while (*++p != '\"') /* ... look for the start of the next segment */
      {
        if(*p == '#') /* check for #...$ comment */
	{
	  while(*++p != '\n') 
          {
            /* skip the rest of the line */
          }
        }
        else if (*p == '/')
        {
          if(*++p == '/') /* check for //...$ comment */
          {
            while (*++p != '\n') 
            {
              /* skip the rest of the line */
            }
          }
          else /* must be '*', lead-in to an old C-style comment */
          {
            while (*++p != '*' || *(p+1) != '/')
            {
              /* skip all comment content */
            }
            ++p; /* step to the trailing slash, to skip it as well */
          }
        }
      }
    }
    else
      *(q++) = *p;
  }

  *q = 0;

  return(r);
}

%}

ws               [ \t\f\r\n]+
equals           \=|\:
comma            ,
group_start      \{
group_end        \}
true             TRUE|true
false            FALSE|false
name             [A-Za-z\*][-A-Za-z0-9_\*]*
quote            \"
integer          [-+]?[0-9]+
hex              0[Xx][0-9A-Fa-f]+
float            ([-+]?([0-9]*)?\.[0-9]*([eE][-+]?[0-9]+)?)|([-+]?([0-9]+)(\.[0-9]*)?[eE][-+]?[0-9]+)
segment          {quote}([^\"\\]|\\.)*{quote}
string           {segment}(([ \t\f\r\n]*((#|\/\/).*\n|\/\*(.|\n)*\*\/)*)*{segment})*
end              ;
array_start      \[
array_end        \]
list_start       \(
list_end         \)
comment          (#|\/\/).*$

%x COMMENT

%%

\/\*          { BEGIN COMMENT; }
<COMMENT>\*\/ { BEGIN INITIAL; }
<COMMENT>.    { /* ignore */ }
<COMMENT>\n   {  }

{ws}          { /* skip */ }

{equals}      { return(TOK_EQUALS); }
{comma}       { return(TOK_COMMA); }
{group_start} { return(TOK_GROUP_START); }
{group_end}   { return(TOK_GROUP_END); }
{true}        { yylval->ival = 1; return(TOK_BOOLEAN); }
{false}       { yylval->ival = 0; return(TOK_BOOLEAN); }
{name}        { yylval->sval = strdup(yytext); return(TOK_NAME); }
{float}       { yylval->fval = atof(yytext); return(TOK_FLOAT); }
{integer}     { yylval->ival = atoi(yytext); return(TOK_INTEGER); }
{hex}         { yylval->ival = strtoul(yytext, NULL, 16); return(TOK_HEX); }
{string}      { yylval->sval = strdup(make_string(yytext)); return(TOK_STRING); }
{array_start} { return(TOK_ARRAY_START); }
{array_end}   { return(TOK_ARRAY_END); }
{list_start}  { return(TOK_LIST_START); }
{list_end}    { return(TOK_LIST_END); }
{end}         { return(TOK_END); }
{comment}     { /* ignore */ }
.             { return(TOK_GARBAGE); }
