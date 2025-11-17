lexer grammar IckleLexer;


@header {
/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc.
 */
}

WS: (' ' | '\t' | '\u000B' | '\f' | EOL)+ -> channel(HIDDEN);

fragment
NL: ('\r' | '\n') ;

fragment
EOL: NL+ ;

// ----------------------
// Literals
// ----------------------
HEX_LITERAL: '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;

INTEGER_LITERAL: ('0' | '1'..'9' '0'..'9'*) ;

DECIMAL_LITERAL: ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX ;

OCTAL_LITERAL: '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

fragment
HEX_DIGIT: ('0'..'9' | 'a'..'f' | 'A'..'F') ;

fragment
INTEGER_TYPE_SUFFIX: ('l'|'L') ;

FLOATING_POINT_LITERAL:
  ('0'..'9')+ '.' ('0'..'9')* EXPONENT? FLOAT_TYPE_SUFFIX?
  |  '.' ('0'..'9')+ EXPONENT? FLOAT_TYPE_SUFFIX?
  |  ('0'..'9')+ EXPONENT FLOAT_TYPE_SUFFIX?
  |  ('0'..'9')+ FLOAT_TYPE_SUFFIX
  ;

fragment
EXPONENT: ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
FLOAT_TYPE_SUFFIX: ('f'|'F') | ('d'|'D') ;

CHARACTER_LITERAL:
  '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\'' { setText(getText().substring(1, getText().length() - 1)); }
  ;

STRING_LITERAL:
  '"' ( ESCAPE_SEQUENCE | ~('\\'|'"') )* '"' { setText(getText().substring(1, getText().length() - 1)); }
  |  ('\'' ( ESCAPE_SEQUENCE | ~('\\'|'\'') )* '\'')+ { setText(getText().substring(1, getText().length() - 1).replace("''", "'")); }
  ;

fragment
ESCAPE_SEQUENCE:
  '\\' [btnfr"'\\]
  |  UNICODE_ESCAPE
  |  OCTAL_ESCAPE
  ;

fragment
OCTAL_ESCAPE:
  '\\' ('0'..'3') ('0'..'7') ('0'..'7')
  |  '\\' ('0'..'7') ('0'..'7')
  |  '\\' ('0'..'7')
  ;

fragment
UNICODE_ESCAPE:
  '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
  ;

REGEXP_LITERAL
   :  '/' ( ~[\r\n\\/]| '\\' ~[\r\n] )* '/'
   { setText(getText().substring(1, getText().length() - 1)); }
   ;

// ----------------------
// Keywords / Operators
// ----------------------
TO: ('t'|'T') ('o|O') ;
TRUE: ('t'|'T') ('r'|'R') ('u'|'U') ('e'|'E') ;
FALSE: ('f'|'F') ('a'|'A') ('l'|'L') ('s'|'S') ('e'|'E') ;
NULL: ('n'|'N') ('u'|'U') ('l'|'L') ('l'|'L') ;
EQUALS: '=' ;
COLON: ':' ;
NOT_EQUAL: '<>' | '!=' ;
PARAM: '?' ;
EXCLAMATION: '!' ;
GREATER: '>' ;
GREATER_EQUAL: '>=' ;
LESS: '<' ;
LESS_EQUAL: '<=' ;
AND: '&&' ;
OR: '||' ;

// ----------------------
// Identifiers
// ----------------------
IDENTIFIER:
  ('a'..'z' | 'A'..'Z' | '_' | '$' | '\u0080'..'\ufffe') ('a'..'z' | 'A'..'Z' | '_' | '$' | '0'..'9' | '\u0080'..'\ufffe')*
  ;

QUOTED_IDENTIFIER:
  '`' ( ESCAPE_SEQUENCE | ~('\\' | '`') )* '`'
  ;

LPAREN: '(' ;
RPAREN: ')' ;
LSQUARE: '[' ;
RSQUARE: ']' ;
LCURLY: '{' ;
RCURLY: '}' ;
COMMA: ',' ;
DOT: '.' ;
PLUS: '+' ;
MINUS: '-' ;
ASTERISK: '*' ;
HASH: '#' ;
TILDE: '~' ;
CARAT: '^' ;
ARROW: '<->' ;