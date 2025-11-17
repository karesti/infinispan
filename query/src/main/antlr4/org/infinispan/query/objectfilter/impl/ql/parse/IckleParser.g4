parser grammar IckleParser;

options {
	tokenVocab=IckleLexer;
}

@parser::header {
/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc.
 */
import java.util.ArrayList;
import java.util.List;
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Statements

/**
 * Toplevel rule, entrypoint to the whole grammar
 */
statement
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
   :  (selectStatement | deleteStatement) EOF
   ;

/**
 * A 'select' query
 */
selectStatement
   :  querySpec orderByClause? (QUERY querySpec orderByClause?)
   ;

/**
 * A 'delete' statement
 */
deleteStatement
   :  deleteClause whereClause?
   ;

deleteClause
   :  delete_key fromClause
   ;

querySpec
   :  selectFrom whereClause? ( groupByClause havingClause? )?  (QUERY_SPEC selectFrom whereClause? groupByClause? havingClause?)
   ;

groupByClause
@init { if (state.backtracking == 0) pushEnableParameterUsage(false); }
@after { popEnableParameterUsage(); }
   :  group_by_key groupingSpecification
   ;

havingClause
	:	having_key logicalExpression
	;

groupingSpecification
	:	groupingValue ( COMMA groupingValue )*
	;

groupingValue
   :  additiveExpression collationSpecification?  (GROUPING_VALUE additiveExpression collationSpecification?)
	;

filteringClause
   :  filtering_key logicalExpression
   ;

whereClause
	:	where_key logicalExpression
	;

selectFrom
    : sc=selectClause? fc=fromClause
    ;

fromClause
    : from_key persisterSpaces
    ;

persisterSpaces
    : ps+=persisterSpace ( COMMA ps+=persisterSpace )*
    ;

persisterSpace
	:	persisterSpaceRoot ( qualifiedJoin | crossJoin )*
	;

crossJoin
	:	cross_key join_key mainEntityPersisterReference
		 (PERSISTER_JOIN cross_key mainEntityPersisterReference)
	;

qualifiedJoin
    : nonCrossJoinType join_key fetch_key? path ac=aliasClause
      (on_key logicalExpression | propertyFetch? withClause?)?
      (PERSISTER_JOIN nonCrossJoinType ENTITY_PERSISTER_REF aliasClause?)?
      (PROPERTY_JOIN nonCrossJoinType fetch_key? aliasClause? propertyFetch? PATH path withClause?)?
    ;

withClause
	:	with_key logicalExpression
	;

nonCrossJoinType
	:	inner_key
	|	outerJoinType outer_key?
	|	 INNER
	;

outerJoinType
	:	left_key
	|	right_key
	|	full_key
	;

persisterSpaceRoot
options {
backtrack=true;
}	:	hibernateLegacySyntax
	|	jpaCollectionReference
	|	mainEntityPersisterReference
	;

mainEntityPersisterReference
    : entityName ac=aliasClause propertyFetch? (ENTITY_PERSISTER_REF entityName aliasClause? propertyFetch?)?
    ;

propertyFetch
   :  fetch_key all_key properties_key
   ;

hibernateLegacySyntax
    : aliasDeclaration in_key
      (collectionExpression
      (PROPERTY_JOIN INNER aliasDeclaration collectionExpression)?)?
    ;

jpaCollectionReference
    : in_key LPAREN propertyReference RPAREN ac=aliasClause
      (PROPERTY_JOIN INNER aliasClause? propertyReference)?
    ;

selectClause
	:	select_key distinct_key? rootSelectExpression
	;

rootSelectExpression
	:	jpaSelectObjectSyntax
	|	explicitSelectList
	;

explicitSelectList
   :   explicitSelectItem ( COMMA explicitSelectItem )*  (SELECT_LIST explicitSelectItem+)
   ;

explicitSelectItem
	:	selectExpression
	;

selectExpression
    : expression ac=aliasClause (SELECT_ITEM expression aliasClause?)?
    ;

aliasClause
    : ALIAS_NAME
    | aliasDeclaration
    | as_key aliasDeclaration
    ;

aliasDeclaration
	:	IDENTIFIER
	;

aliasReference
	:	IDENTIFIER
	;

jpaSelectObjectSyntax
	:	object_key LPAREN aliasReference RPAREN  (SELECT_ITEM aliasReference)
	;

orderByClause
   :  order_by_key sortSpecification ( COMMA sortSpecification )*
   ;

sortSpecification
	:  sortKey collationSpecification? orderingSpecification  (SORT_SPEC sortKey collationSpecification? orderingSpecification)
	;

orderingSpecification
   returns [String order]
	:	ascending_key  { $order = "asc"; }
	|	descending_key { $order = "desc"; }
	|  o=ORDER_SPEC   { $order = $o.getText(); }
	;

sortKey
@init	{ if (state.backtracking == 0) pushEnableParameterUsage(false); }
@after { popEnableParameterUsage(); }
//PARAMETERS CAN'T BE USED  This verification should be scoped
	:	additiveExpression
	;

collationSpecification
    returns [String collation]
  : COLLATE_KEY n=collateName { $collation = $n.text; }
  ;

collateName
	:	dotIdentifierPath
	;

logicalExpression
	:	expression
	;

expression
	:	logicalOrExpression
	;

logicalOrExpression
	:	logicalAndExpression ( or_key logicalAndExpression )*
	;

logicalAndExpression
	:	negatedExpression ( and_key negatedExpression )*
	;

negatedExpression
   :  not_key negatedExpression
   |  equalityExpression
   ;

equalityExpression
    locals [boolean isNull = false, boolean isNegated = false]
    : fullTextExpression
    | knnExpression
    | relationalExpression equalityTail*
    ;

// null/empty
equalityTail
    : isClause
    | comparisonClause
    ;

// IS NULL, IS NOT NULL, IS EMPTY, IS NOT EMPTY
isClause
    : IS_KEY (NOT_KEY)?
      (NULL | EMPTY_KEY)
    ;

// (=, !=)
comparisonClause
    : op=(EQUALS | NOT_EQUAL) relationalExpression
    ;

relationalExpression
locals [boolean isNegated = false]
    : additiveExpression relationalTail*
    ;

relationalTail
    : op=(LESS | GREATER | LESS_EQUAL | GREATER_EQUAL) additiveExpression
    | not_key? relationalOperator
    ;

relationalOperator
    : in_key inList
    | between_key betweenList
    | like_key additiveExpression likeEscape?
    | member_of_key path
    | within_key geoShape
    ;

geoShape
    : geoCircle
    | geoBoundingBox
    | geoPolygon
    ;

geoCircle
    returns [double lat, double lon, double radius]
    : circle_key
      LPAREN
      latAtom=atom COMMA
      lonAtom=atom COMMA
      radiusAtom=distanceVal
      RPAREN
    ;

geoBoundingBox
    returns [double tlLat, double tlLon, double brLat, double brLon]
    : boundingBox_key
      LPAREN
      tlLatAtom=atom COMMA
      tlLonAtom=atom COMMA
      brLatAtom=atom COMMA
      brLonAtom=atom
      RPAREN
    ;

geoPolygon
    returns [Object polygonExpr]
    : polygon_key LPAREN expr=expressionOrVector RPAREN
    ;

likeEscape
	:  escape_key additiveExpression
	;

inList
	:  collectionExpression  (IN_LIST collectionExpression)
	|  LPAREN additiveExpression (COMMA additiveExpression)* RPAREN  (IN_LIST additiveExpression+)
	;

betweenList
    returns [Object lower, Object upper]
    : lowerExpr=additiveExpression
      and_key
      upperExpr=additiveExpression
    ;


additiveExpression
   :  quantifiedExpression
   |  standardFunction
   |  distanceFunction
   |  setFunction
   |  versionFunction
   |  scoreFunction
   |  collectionExpression
   |  atom
   ;

distanceFunction
   : distance_key LPAREN propertyReference COMMA lat=atom COMMA lon=atom distanceFunctionUnit? RPAREN
   ;

distanceFunctionUnit
   : COMMA unit=unitVal
   ;

quantifiedExpression
   :  (some_key | exists_key | all_key | any_key) (collectionExpression | aliasReference)
   ;

standardFunction
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
   :  sizeFunction
   |  indexFunction
   ;

sizeFunction
   :   size_key LPAREN propertyReference RPAREN
   ;

indexFunction
   :   index_key LPAREN aliasReference RPAREN
   ;

versionFunction
   :   version_key LPAREN aliasReference RPAREN
   ;

scoreFunction
   :   score_key LPAREN aliasReference RPAREN
   ;

setFunction
@init { boolean generateOmittedElement = true; if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
	:	( sum_key | avg_key | max_key | min_key ) LPAREN additiveExpression RPAREN
	|	count_key LPAREN ( ASTERISK {generateOmittedElement = false;} | ( ( (distinct_key | all_key) {generateOmittedElement = false;} )? countFunctionArguments ) ) RPAREN
		 {generateOmittedElement}? (count_key ASTERISK? ALL countFunctionArguments?)
		 (count_key ASTERISK? distinct_key? all_key? countFunctionArguments?)
	;

countFunctionArguments
	:	propertyReference
	|	collectionExpression
	|	signedNumericLiteral
	;

collectionExpression
   :   (elements_key | indices_key) LPAREN propertyReference RPAREN
   ;

vectorSearch
   : LSQUARE expressionOrVector RSQUARE
   ;

atom
   :  identPrimary  (PATH identPrimary)
	    //TODO  if ends with:
	    //  .class  class type
	    //  if contains "()" it is a function call
	    //  if it is constantReference (using context)
	    //  otherwise it will be a generic element to be resolved on the next phase (1st tree walker)
   |  constant
   |  parameterSpecification { if (!isParameterUsageEnabled()) throw new RecognitionException(input); }
	//validate using Scopes if it is enabled or not to use parameterSpecification.. if not generate an exception
   |  LPAREN expressionOrVector RPAREN
   |  vectorSearch
   ;

distanceVal
   :  constant unit=unitVal?
   |  parameterSpecification unit=unitVal? { if (!isParameterUsageEnabled()) throw new RecognitionException(input); }
	//validate using Scopes if it is enabled or not to use parameterSpecification.. if not generate an exception
   ;

unitVal
   : meters_key
   | kilometers_key
   | miles_key
   | yards_key
   | nautical_miles_key
   ;

parameterSpecification
    : COLON id=IDENTIFIER
    | PARAM intVal=INTEGER_LITERAL
    | PARAM
    ;

expressionOrVector
@init {boolean isVectorExp = false;}
	:	expression (vectorExpr {isVectorExp = true;})?
		 {isVectorExp}? (VECTOR_EXPR expression vectorExpr)
		 expression
	;

vectorExpr
@init	{ if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after	{ popEnableParameterUsage(); }
	:	COMMA expression (COMMA expression)*
	;

identPrimary
	: 	IDENTIFIER
		(	DOT IDENTIFIER
		|	LSQUARE expression RSQUARE
		|	LSQUARE RSQUARE
		|	LPAREN exprList RPAREN
		)*
	;

exprList
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
	:	expression (COMMA expression)*
	|
	;

constant
   :  booleanLiteral
   |  stringLiteral
   |  signedNumericLiteral
   |  NULL
   ;

stringLiteral
   :  CHARACTER_LITERAL  (CONST_STRING_VALUE CHARACTER_LITERAL)
   |  STRING_LITERAL  (CONST_STRING_VALUE STRING_LITERAL)
   ;

booleanLiteral
   :  TRUE
   |  FALSE
   ;

numericLiteral
   :  INTEGER_LITERAL
   |  DECIMAL_LITERAL
   |  FLOATING_POINT_LITERAL
   |  HEX_LITERAL
   |  OCTAL_LITERAL
   ;

signedNumericLiteral
   :  (MINUS | PLUS) numericLiteral
   |  numericLiteral
   ;

entityName
    : dip=dotIdentifierPath
    ;

propertyReference
	:	path  (PROPERTY_REFERENCE path)
	;

dotIdentifierPath
	:	IDENTIFIER (DOT IDENTIFIER)*
	;

path
	:	IDENTIFIER
		(	DOT IDENTIFIER
		|	LSQUARE expression RSQUARE
		|	LSQUARE RSQUARE
		)*
	;

object_key
   :   IDENTIFIER
	;

sum_key
   :   IDENTIFIER
	;

avg_key
   :   IDENTIFIER
	;

max_key
   :   IDENTIFIER
	;

min_key
   :   IDENTIFIER
	;

count_key
   :   IDENTIFIER
	;

version_key
   :   IDENTIFIER
	;

score_key
   :   IDENTIFIER
	;

size_key
   :   IDENTIFIER
	;

index_key
   :   IDENTIFIER
	;

any_key
   :   IDENTIFIER
	;

exists_key
   :   IDENTIFIER
	;

some_key
   :   IDENTIFIER
	;

escape_key
   :   IDENTIFIER
	;

like_key
   :   IDENTIFIER
	;

between_key
   :   IDENTIFIER
	;

member_of_key
	:   id=IDENTIFIER IDENTIFIER
	;

empty_key
   :   IDENTIFIER
	;

is_key
   :   IDENTIFIER
   ;

or_key
   :   OR
   |   IDENTIFIER
	;

and_key
   :   AND
   |   IDENTIFIER
	;

not_key
   :   EXCLAMATION
   |   IDENTIFIER
   ;

to_key
   :   IDENTIFIER
   ;

having_key
   :   IDENTIFIER
	;

filtering_key
   :   IDENTIFIER
	;

with_key
   :   IDENTIFIER
	;

within_key
   :   IDENTIFIER
   ;

distance_key
   :   IDENTIFIER
   ;

circle_key
   :   IDENTIFIER
   ;

boundingBox_key
   :   IDENTIFIER
   ;

polygon_key
   :   IDENTIFIER
   ;

on_key
   :   IDENTIFIER
	;

meters_key
   :  IDENTIFIER
   ;

kilometers_key
   :  IDENTIFIER
   |  IDENTIFIER
   ;

miles_key
   :  IDENTIFIER
   ;

yards_key
   :  IDENTIFIER
   ;

nautical_miles_key
   :  IDENTIFIER
   ;

indices_key
   :   IDENTIFIER
	;

cross_key
   :   IDENTIFIER
	;

join_key
   :   IDENTIFIER
	;

inner_key
   :   IDENTIFIER
	;

outer_key
   :   IDENTIFIER
	;

left_key
   :   IDENTIFIER
	;

right_key
   :   IDENTIFIER
	;

full_key
   :   IDENTIFIER
	;

elements_key
   :   IDENTIFIER
	;

properties_key
   :   IDENTIFIER
	;

fetch_key
   :   IDENTIFIER
	;

in_key
   :   IDENTIFIER
	;

as_key
   :   IDENTIFIER
	;

where_key
   :   IDENTIFIER
	;

select_key
   :   IDENTIFIER
	;

delete_key
   :   IDENTIFIER
   ;

distinct_key
   :   IDENTIFIER
	;

all_key
   :   IDENTIFIER
	;

ascending_key
   :   IDENTIFIER
	;

descending_key
   :   IDENTIFIER
	;

collate_key
   :   IDENTIFIER
	;

order_by_key
   :   id=IDENTIFIER IDENTIFIER
	;

group_by_key
   :   id=IDENTIFIER IDENTIFIER
	;

from_key
   :   IDENTIFIER
	;

/* TODO [anistor]
DIFFERENCES TO LUCENE SYNTAX:
 - whitespace is not significant.
 - no wildcard (*) for field names, so *:* is not a valid query.
 - a field name/path is always specified. we do not have a default field like lucene has.
 - = operator is not accepted instead of : as in lucene StandardSyntaxParser
 - &&, || are accepted alternatives for AND/OR, in both full-text and jpa predicates
 - ! can be used instead of NOT
 - AND,OR,NOT are capitalised in lucene but in infinispan they are case insensitive
 - when and/or is missing, OR will be assumed
 - string terms must be enclosed in single or double quotes. lucene allows single-word terms to be unquoted
 - fuzziness and boosting are not accepted in arbitrary order as in lucene's parser. fuzziness must always come first.
 - we accept != instead of <> from jpa
 - >,>=,<,<= operators work as in JPA not as in lucene's StandardSyntaxParser so boosting cannot be applied. use ranges to achieve that.
*/

fullTextExpression
   :  ftOccurrence ftFieldPath COLON ftBoostedQuery  (COLON ftFieldPath (ftOccurrence ftBoostedQuery))
   |  ftFieldPath COLON ftBoostedQuery
   ;

knnExpression
   : vectorFieldPath ARROW knnTerm
   ;

ftOccurrence
    : plus=PLUS
    | minus=MINUS
    | excl=EXCLAMATION
    | hash=HASH
    | nk=not_key
    ;

ftFieldPath
   :  dotIdentifierPath  (PATH dotIdentifierPath)
   ;

vectorFieldPath
   :  dotIdentifierPath  (PATH dotIdentifierPath)
   ;

ftBoostedQuery
   :  ftTermOrQuery ftBoost?
   ;

ftBoost
    : c1=CARAT val=ftNumericLiteralOrParameter c2=CARAT
    ;

ftTermOrQuery
    : ftTerm
    | ftRange
    | LPAREN ftConjunction (ftOr ftConjunction)* RPAREN
    ;

ftTerm
   :  ftLiteralOrParameter ftFuzzySlop?  (FT_TERM ftLiteralOrParameter ftFuzzySlop?)
   |  REGEXP_LITERAL  (FT_REGEXP REGEXP_LITERAL)
   ;

knnTerm
   :  vectorSearch knnDistance? filteringClause?  (KNN_TERM vectorSearch knnDistance? filteringClause?)
   ;

ftFuzzySlop
    : t1=TILDE val=ftNumericLiteralOrParameter? t2=TILDE
    ;

ftRange
    : rb=ftRangeBegin lower=ftRangeBound tk=to_key? upper=ftRangeBound re=ftRangeEnd
    ;

ftRangeBegin
   :  LSQUARE
   |  LCURLY
   ;

ftRangeEnd
   :  RSQUARE
   |  RCURLY
   ;

ftRangeBound
   :  ASTERISK
   |  ftLiteralOrParameter
   ;

ftOr
   :  or_key
   |   OR
   ;

ftConjunction
   :  ftClause (and_key ftClause)*
   ;

ftClause
   :  (options { greedy=true; } : ftOccurrence)? ftBoostedQuery
   ;

ftLiteralOrParameter
   :  parameterSpecification
   |  stringLiteral
   |  signedNumericLiteral
   ;

ftNumericLiteralOrParameter
   :  parameterSpecification
   |  numericLiteral
   ;

knnDistance
    : t1=TILDE val=ftNumericLiteralOrParameter? t2=TILDE
    ;

nakedIdentifier
   :IDENTIFIER
   | JOIN
   | INDEX
   | VERSION
   | SOME
   | SCORE
   | SIZE
   | INDEX
   | ANY
   | COUNT
   | MIN
   ;
