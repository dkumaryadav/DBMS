#ifndef TABLES_H
#define TABLES_H

#include "dt.h"

// Data Types, Records, and Schemas
typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

typedef struct Value {
  DataType dt;
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
} Value;

typedef struct RID {
  int page;
  int slot;
} RID;

typedef struct Record
{
  RID id;
  char *data;
} Record;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
  int keySize;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
  char *name;
  Schema *schema;
  void *mgmtData;
} RM_TableData;

#define MAKE_STRING_VALUE(result, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = DT_STRING;						\
    (result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
    strcpy((result)->v.stringV, value);					\
  } while(0)


#define MAKE_VALUE(result, datatype, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = datatype;						\
    switch(datatype)							\
      {									\
      case DT_INT:							\
	(result)->v.intV = value;					\
	break;								\
      case DT_FLOAT:							\
	(result)->v.floatV = value;					\
	break;								\
      case DT_BOOL:							\
	(result)->v.boolV = value;					\
	break;								\
      }									\
  } while(0)

// dynamic string
typedef struct VarString {
  char *buf;
  int size;
  int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
  do {							\
  var = (VarString *) malloc(sizeof(VarString));	\
  var->size = 0;					\
  var->bufsize = 100;					\
  var->buf = malloc(100);				\
  } while (0)

#define FREE_VARSTRING(var)			\
  do {						\
  free(var->buf);				\
  free(var);					\
  } while (0)

#define GET_STRING(result, var)			\
  do {						\
    result = malloc((var->size) + 1);		\
    memcpy(result, var->buf, var->size);	\
    result[var->size] = '\0';			\
  } while (0)

#define RETURN_STRING(var)			\
  do {						\
    char *resultStr;				\
    GET_STRING(resultStr, var);			\
    FREE_VARSTRING(var);			\
    return resultStr;				\
  } while (0)

#define ENSURE_SIZE(var,newsize)				\
  do {								\
    if (var->bufsize < newsize)					\
    {								\
      int newbufsize = var->bufsize;				\
      while((newbufsize *= 2) < newsize);			\
      var->buf = realloc(var->buf, newbufsize);			\
    }								\
  } while (0)

#define APPEND_STRING(var,string)					\
  do {									\
    ENSURE_SIZE(var, var->size + strlen(string));			\
    memcpy(var->buf + var->size, string, strlen(string));		\
    var->size += strlen(string);					\
  } while(0)

#define APPEND(var, ...)			\
  do {						\
    char *tmp = malloc(10000);			\
    sprintf(tmp, __VA_ARGS__);			\
    APPEND_STRING(var,tmp);			\
    free(tmp);					\
  } while(0)


// debug and read methods
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

#endif
