#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

#define RC_FILE_PRESENT 6
#define RC_STORAGE_MGR_NOT_INIT 5
#define RC_INVALID_PAGE_NUMBER 7
#define RC_NULL_ARGUMENT 11
#define RC_READ_FAILED 600
#define RC_DELETE_FAILED 601

#define RC_NO_TUPLES 206

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 0

#define RC_NO_MORE_SPACE_IN_BUFFER 401
#define RC_UNKNOWN_STRATEGY 402
#define RC_INVALID_BM 403
#define RC_NON_EXISTING_PAGE_IN_FRAME 404
#define RC_TABLE_EXISTS 405
#define RC_TABLE_DOES_NOT_EXIST 406

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
do {			  \
RC_message=message;	  \
return rc;		  \
} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
do {									\
int rc_internal = (code);						\
if (rc_internal != RC_OK)						\
{									\
char *message = errorMessage(rc_internal);			\
printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
free(message);							\
exit(1);							\
}									\
} while(0);


#endif
