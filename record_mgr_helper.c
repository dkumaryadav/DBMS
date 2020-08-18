#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;

#define MAKE_VARSTRING(var)                \
        do {                            \
            var = (VarString *) malloc(sizeof(VarString));    \
            var->size = 0;                    \
            var->bufsize = 100;                    \
            var->buf = calloc(100,0);                \
        } while (0)

#define FREE_VARSTRING(var)            \
        do {                        \
            free(var->buf);                \
            free(var);                    \
        } while (0)

#define GET_STRING(result, var)            \
        do {                        \
            result = malloc((var->size) + 1);        \
            memcpy(result, var->buf, var->size);    \
            result[var->size] = '\0';            \
        } while (0)

#define RETURN_STRING(var)            \
        do {                        \
            char *resultStr;                \
            GET_STRING(resultStr, var);            \
            FREE_VARSTRING(var);            \
            return resultStr;                \
        } while (0)

#define ENSURE_SIZE(var,newsize)                \
        do {                                \
            if (var->bufsize < newsize)                    \
            {                                \
                int newbufsize = var->bufsize;                \
                while((newbufsize *= 2) < newsize);            \
                var->buf = realloc(var->buf, newbufsize);            \
            }                                \
        } while (0)

#define APPEND_STRING(var,string)                    \
        do {                                    \
            ENSURE_SIZE(var, var->size + strlen(string));            \
            memcpy(var->buf + var->size, string, strlen(string));        \
            var->size += strlen(string);                    \
        } while(0)

#define APPEND(var, ...)            \
        do {                        \
            char *tmp = malloc(10000);            \
            sprintf(tmp, __VA_ARGS__);            \
            APPEND_STRING(var,tmp);            \
            free(tmp);                    \
        } while(0)

RC attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;

    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
        {
        case DT_STRING:
            offset += schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
        }

    *result = offset;
    return RC_OK;
}