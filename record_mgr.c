//
//  main.c
//  RecordManager
//
//  Created by Deepak Kumar Yadav; Shantanoo Sinha; Nirav Soni on 10/30/19.
//  Copyright © 2019 DeepakKumarYadav. All rights reserved.
//

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
//#include <unistd.h>
#include <math.h>

/* This will be used to check if record manager is initalized or not
 0: Not initalized 1: Initialized */
int init_record_manager = 0;
SM_FileHandle file_handle;

/* Used in scan functions*/
typedef struct recInformation {
    // Slot information
    int current_slot;
    int number_of_slots;
    // Page information
    int current_page;
    int number_of_pages;
    // Search condition used for scanning
    Expr *srch_cond;
}recInformation;

/* Used for storing table information */
typedef struct tblManagement {
    BM_BufferPool *bm;
    int no_of_tuples;
    int lenght_of_slot;
    int maximum_slots;
    int rec_start_position;
    int rec_last_position;
    int length_of_schema;
}tblManagement;

// Additional functions used in the code
tblManagement* initTableSchema(tblManagement *table_info, Schema *schema);
int writePage(SM_FileHandle file_handle, char *tble_in_strng);
int writeSchema(SM_FileHandle file_handle, char *schema_in_strng);
int freeMemory( void *pointer);

// Main Method
/* int main(){
 printf("Hello, World!\n");
 return 0;
 } */

/* Initialize record manager if init_record_manager = 0 this means
 record manager is not initalized */
extern RC initRecordManager (void *mgmtData){
    // Chceking if record manager is already initalized or not
    if ( init_record_manager == 0 ){
        init_record_manager = 1;
        initStorageManager();
        printf(" Record Manager initialization Completed \n");
    }
    return RC_OK;
}

/* Shutdown record manager by making init_record_manager = 0 */
extern RC shutdownRecordManager (){
    // Update value of init_record_manager to 0
    init_record_manager = 0;
    printf("Record Manager shut down successfully \n");
    return RC_OK;
}

/* Function used to calculate length of schema */
int getSchemaLength(Schema *schema) {
    /* As the schema is fixed we refer its defination to come up with its length
     typedef struct Schema {
     int numAttr;
     char **attrNames;
     DataType *dataTypes;
     int *typeLength;
     int *keyAttrs;
     int keySize;
     } Schema; */
    int length = 0;
    length = length +sizeof(int);
    length = length +sizeof(int)*schema->numAttr;
    length = length +sizeof(int)*schema->numAttr;
    length = length +sizeof(int)*schema->numAttr;
    length = length +sizeof(int);
    
    // Updating length with attribute names length
    if( schema-> numAttr > 0 ){
        int counter =0;
        do{
            length = length + (int) strlen(schema->attrNames[counter]);
            counter++;
        }while( counter< schema->numAttr);
    }
    
    return length;
}

/* Function used to calculate length of slot */
int getSlotSize(Schema *schema) {
    int size= 15, tmp_size=0;
    
    if ( schema-> numAttr > 0){
        int i=0;
        do{
            if( schema->dataTypes[i] == DT_STRING)   // If data type is string
                tmp_size = schema->typeLength[i];
            else if (schema->dataTypes[i] == DT_INT || schema->dataTypes[i] == DT_BOOL) // if data type is int or boolean
                tmp_size = 5;
            else if (schema->dataTypes[i] == DT_FLOAT) // If datat type is float
                tmp_size = 10;
            
            size = size + (tmp_size + (int) strlen(schema -> attrNames[i]) + 2);
            i++;
        }while(i<schema->numAttr);
    }
    
    return size;
}

/*Function to convert table information into string to persist to file */
char* tableToString(tblManagement *tbl_mgmt) {
    
    VarString *string;
    MAKE_VARSTRING(string);
    char *final_string;
    APPEND(string, "SchemaLength {%i} FirstPage {%i} LastPage {%i} Tuples {%i} SlotLength {%i} MaxSlots {%i} \n", tbl_mgmt->length_of_schema, tbl_mgmt->rec_start_position, tbl_mgmt->rec_last_position, tbl_mgmt->no_of_tuples, tbl_mgmt->lenght_of_slot, tbl_mgmt->maximum_slots);
    GET_STRING(final_string, string);
    printf("%s", final_string);
    return (final_string);
}


/* Function used to initialize table and schema information */
tblManagement* initTableSchema(tblManagement *table_info, Schema *schema){
    
    int len_file;
    int len_slot;
    int max_slot;
    int len_schema;
    
    // Populate information related to table
    len_schema = getSchemaLength(schema);
    len_file =((int) ((float)len_schema / PAGE_SIZE)) +1;
    len_slot = getSlotSize(schema);
    max_slot = ((int) ((float)PAGE_SIZE/(float)len_slot)) -1;
    
    table_info -> no_of_tuples = 0;
    table_info -> rec_start_position = len_file +1;
    table_info -> lenght_of_slot = len_slot;
    table_info -> rec_last_position = len_file + 1;
    table_info -> maximum_slots = max_slot;
    table_info -> length_of_schema = len_schema;
    
    return table_info;
}

// Write table data to page 0
int writePage(SM_FileHandle file_handle, char *tble_in_strng){
    printf("Write Page: START");
    int return_code = 0;
    return_code = writeBlock(0, &file_handle, tble_in_strng);
    if( return_code != RC_OK)
        return return_code;
    printf("Write Page: Completed");
    return RC_OK;
}

// Write schema data to page 1
int writeSchema(SM_FileHandle file_handle, char *schema_in_strng){
    printf("Write Schema: START");
    int return_code = 0;
    return_code = writeBlock(1, &file_handle, schema_in_strng);
    if( return_code != RC_OK)
        return return_code;
    printf("Write Schema: Completed");
    return RC_OK;
}

/* Creating a table and store table and schema info as string */
extern RC createTable (char *name, Schema *schema){
    printf("createTable: STARTED\n");
    // Declare variables to be used in code
    FILE *file;
    char *tble_in_strng, *schema_in_strng;
    int return_code = 0, length_of_table = sizeof(tblManagement);
    tblManagement *table_info = (tblManagement *) malloc(length_of_table);
    
    // Checking if file exists or not. If exists return EC:
    if( (file = fopen(name, "r")) != NULL )
        return RC_TABLE_EXISTS;
    else
        fclose(file);
    
    // In the first page(page:0) we need to write file infomration
    // Create a page file
    printf("Calling creating page\n");
    return_code = createPageFile(name);
    if (return_code != RC_OK)
        return return_code;
    printf("createTable: initTableSchema\n");
    // Initialize table related information
    table_info = initTableSchema(table_info, schema);
    
    printf("createTable: Open Page File\n");
    return_code = openPageFile(name, &file_handle);
    if (return_code != RC_OK)
        return return_code;
    
    int length_of_file = table_info -> rec_start_position;
    printf("createTable: Capacity Check\n");
    return_code = ensureCapacity(length_of_file, &file_handle);
    if (return_code != RC_OK)
        return return_code;
    
    // Convert table information to string
    printf("createTable: table to string \n");
    tble_in_strng = tableToString(table_info);
    printf("createTable: schema to string \n");
    schema_in_strng = serializeSchema(schema);
    
    // Write table related information in page-0
    printf("createTable: write page\n");
    return_code = writePage(file_handle, tble_in_strng);
    if ( return_code != RC_OK)
        return return_code;
    // Write schema related information in page-1
    printf("createTable: write schema\n");
    return_code = writeSchema(file_handle, schema_in_strng);
    if ( return_code != RC_OK)
        return return_code;
    
    // Close the file_handler;
    printf("createTable: closePageFile\n");
    return_code = closePageFile(&file_handle);
    if ( return_code != RC_OK)
        return return_code;
    
    printf("createTable: COMPLETED\n");
    return RC_OK;
}

/* Function used to convert string into table information */
tblManagement *strToTableInfo(char *table_string) {
    
    printf("strToTableInfo: START \n");
    int BASE = 10, table_mgmt_size = sizeof(tblManagement);
    tblManagement *table_data = (tblManagement*) malloc (table_mgmt_size);
    int table_length = strlen(table_string) ;
    char table_data_string[table_length];
    
    printf(" strToTableInfo: string copy \n");
    strcpy( table_data_string, table_string);
    printf("%s", table_data_string);
    
    char *temp_string1 = NULL, *temp_string2 = NULL;
    temp_string1 = strtok(table_data_string,"{");
    temp_string1 = strtok(NULL,"}");
    
    printf("strToTableInfo: %ld", strtol(temp_string1, &temp_string2, 10));
    table_data -> length_of_schema = strtol(temp_string1, &temp_string2, BASE);
    
    printf("strToTableInfo: conversion started\n");
    for( int i=0; i< 5; i++){
        printf("strToTableInfo: string tok started\n");
        temp_string1 = strtok(NULL,"{");
        temp_string1 = strtok(NULL,"}");
        
        if( i == 0){
            table_data -> rec_start_position = (int) strtol(temp_string1, &temp_string2, BASE);
            printf("strToTableInfo: rec_start_position converted \n");
        } else if( i == 1 ){
            table_data -> rec_last_position = (int) strtol(temp_string1, &temp_string2, BASE);
            printf("strToTableInfo: rec_last_position converted \n");
        } else if( i == 2){
            table_data -> no_of_tuples = (int) strtol(temp_string1, &temp_string2, BASE);
            printf("strToTableInfo: no_of_tuples converted \n");
        } else if ( i == 3 ){
            table_data -> lenght_of_slot = (int) strtol(temp_string1, &temp_string2, BASE);
            printf("strToTableInfo: lenght_of_slot converted \n");
        } else if ( i == 4) {
            table_data -> maximum_slots = (int) strtol(temp_string1, &temp_string2, BASE);
            printf("strToTableInfo: maximum_slots converted \n");
        }
    }
    
    printf("strToTableInfo: Completed \n");
    return table_data;
}

/* Function used to convert string into schema */
Schema *stringToSchema(char *str_schema) {
    printf("stringToSchema: START\n");
    int BASE =10;
    int size_of_schema = sizeof(Schema);
    int length_of_str_schema = (int)strlen(str_schema);
    int return_code = 0;
    Schema *schema = (Schema*) malloc(size_of_schema);
    
    char schema_info[(int)strlen(str_schema)];
    // Copying string_schema into schema_info
    int SIZE_OF_CHAR = sizeof(char);
    int SIZE_OF_CHAR_PTR = sizeof(char *);
    int SIZE_OF_DATA_TYPE = sizeof(DataType);
    int SIZE_OF_INT = sizeof(int);
    int counter = 0;
    do{
        schema_info[counter] = str_schema[counter];
        counter++;
    }while(str_schema[counter] != '\0');
    
    char *temp_string1 = NULL;
    temp_string1 = strtok(schema_info, "<");
    temp_string1= strtok(NULL, ">");
    char *temp_string2 = NULL;
    int no_attributes = (int) strtol(temp_string1, &temp_string2, BASE);
    int size_attr_names = SIZE_OF_CHAR_PTR * no_attributes;
    int size_data_types = SIZE_OF_DATA_TYPE * no_attributes;
    int size_type_lgt = SIZE_OF_INT * no_attributes;
    schema->attrNames=(char **)malloc(size_attr_names);
    schema->numAttr = no_attributes;
    schema->typeLength=(int*) malloc(size_type_lgt);
    schema->dataTypes=(DataType *)malloc(size_data_types);
    
    char* reference[no_attributes];
    temp_string1 = strtok(NULL, "(");
    
    for(int counter = 0; counter < no_attributes; counter++) {
        temp_string1 = strtok(NULL, ": ");
        schema->attrNames[counter] = (char*) calloc( strlen(temp_string1), SIZE_OF_CHAR);
        
        strcpy(schema->attrNames[counter], temp_string1);
        int temp = no_attributes-1;
        if( counter == temp)
            temp_string1 = strtok(NULL, ") ");
        else
            temp_string1 = strtok(NULL, ", ");
        
        reference[counter] = (char *)calloc( strlen(temp_string1), SIZE_OF_CHAR);
        
        if(strcmp(temp_string1, "INT")==0) {
            schema->typeLength[counter]=0;
            schema->dataTypes[counter]=DT_INT;
        } else if(strcmp(temp_string1, "BOOL")==0) {
            schema->typeLength[counter]=0;
            schema->dataTypes[counter]=DT_BOOL;
            
        } else if(strcmp(temp_string1, "FLOAT")==0) {
            schema->dataTypes[counter]=DT_FLOAT;
            schema->typeLength[counter]=0;
        } else
            strcpy(reference[counter], temp_string1);
    }
    
    counter = 0;
    char* key_attributes[no_attributes];
    int size=0;
    
    if( (temp_string1 = strtok(NULL, "(")) != NULL) {
        char *keystr = strtok(temp_string1 , ", ");
        temp_string1 = strtok(NULL, ")");
        int temp = strlen(keystr)*SIZE_OF_CHAR;
        
        while(keystr!=NULL){
            key_attributes[size]=(char*)malloc(temp);
            strcpy(key_attributes[size], keystr);
            size = size + 1;
            keystr=strtok(NULL, ", ");
        }
        counter = 1;
    }
    
    if( no_attributes > 0){
        char* temp_string3 = NULL;
        for(int loop_counter=0; loop_counter< no_attributes; loop_counter++) {
            if(strlen(reference[loop_counter])>0) {
                temp_string3 = (char*)malloc(SIZE_OF_CHAR*strlen(reference[loop_counter]));
                memcpy(temp_string3, reference[loop_counter], strlen(reference[loop_counter]));
                schema->dataTypes[loop_counter]= DT_STRING;
                temp_string1 = strtok(temp_string3, "[");
                temp_string1 = strtok(NULL,"]");
                schema->typeLength[loop_counter]= (int) strtol(temp_string1, &temp_string2, BASE);
                return_code = freeMemory( reference[loop_counter] );
                if( return_code != RC_OK )
                    printf("%d",return_code);
                return_code = freeMemory( temp_string3 );
                if( return_code != RC_OK )
                    printf("%d",return_code);
            }
        }
    }
    
    if(counter == 1) {
        int temp = SIZE_OF_INT *size;
        schema->keySize = size;
        schema->keyAttrs = (int*) malloc(temp);
        if( size > 0){
            for(int loop_counter1 = 0; loop_counter1 < size; loop_counter1++) {
                for(int loop_counter2=0; loop_counter2 <no_attributes; loop_counter2++) {
                    if(strcmp(key_attributes[loop_counter1], schema->attrNames[loop_counter2])==0) {
                        schema->keyAttrs[loop_counter1] = loop_counter2;
                        return_code = freeMemory(key_attributes[loop_counter1]);
                        if( return_code != RC_OK )
                            printf("%d",return_code);
                    }
                }
            }
        }
    }
    
    printf("stringToSchema: Completed\n");
    return schema;
}

/* Opening a table */
extern RC openTable (RM_TableData *rel, char *name){
    
    printf(" openTable: START \n");
    // Declare variables to be used in code
    FILE *file;
    int size_of_page_handle = sizeof(BM_PageHandle);
    int size_of_buffer_pool = sizeof(BM_BufferPool);
    BM_PageHandle *pHandler = (BM_PageHandle *)malloc(size_of_page_handle);
    int no_of_pages = 3;
    int rep_stratergy = RS_FIFO;
    int pin_page_no = 0;
    BM_BufferPool *bManager = (BM_BufferPool *)malloc(size_of_buffer_pool);
    int return_code = 0;
    char *file_name = name;
    
    // Checking if file exists or not. If does not exist return EC:
    printf(" openTable: table check \n");
    if( (file = fopen(name, "r")) == NULL )
        return RC_TABLE_DOES_NOT_EXIST;
    else
        fclose(file);
    
    printf("openTable: Initializing buffer pool \n");
    return_code = initBufferPool(bManager, file_name, no_of_pages, rep_stratergy, NULL);
    if( return_code != 0)
        return return_code;
    
    printf("openTable: pin page \n");
    return_code = pinPage(bManager, pHandler, pin_page_no);
    if( return_code != 0)
        return return_code;
    
    printf("openTable: string to table\n");
    printf("%s\n", pHandler->data);
    tblManagement *table_info = strToTableInfo(pHandler->data);
    
    if( table_info -> length_of_schema < PAGE_SIZE){
        pin_page_no = pin_page_no +1;
        return_code = pinPage(bManager, pHandler, pin_page_no);
        if( return_code != 0)
            return return_code;
    }
    
    printf("openTable: string to schema\n");
    rel -> name = name;
    rel -> schema = stringToSchema(pHandler->data);
    table_info -> bm = bManager;
    rel->mgmtData = table_info;
    
    return_code = freeMemory(pHandler);
    if( return_code != RC_OK)
        return return_code;
    
    printf(" openTable: Completed \n");
    return RC_OK;
}

/* Free Memory */
int freeMemory( void *pointer){
    printf("freeMemory: START \n");
    free(pointer);
    printf("freeMemory: Completed \n");
    return RC_OK;
}
/* Close table*/
extern RC closeTable (RM_TableData *rel){
    /* Defination of RM_TableData is
     typedef struct RM_TableData {
     char *name;
     Schema *schema;
     void *mgmtData;
     } RM_TableData;
     
     typedef struct Schema {
     int numAttr;
     char **attrNames;
     DataType *dataTypes;
     int *typeLength;
     int *keyAttrs;
     int keySize;
     }; */
    printf("closeTable: START \n");
    int return_code;
    tblManagement *table_mgmt = rel-> mgmtData;
    BM_BufferPool *BM = table_mgmt -> bm;
    
    printf("closeTable shutdownBufferPool\n");
    return_code = shutdownBufferPool(BM);
    if(return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> schema -> attrNames\n");
    return_code = freeMemory( rel -> schema -> attrNames);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> schema -> typeLength\n");
    return_code = freeMemory(rel -> schema -> typeLength);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> schema -> keyAttrs\n");
    return_code = freeMemory(rel -> schema -> keyAttrs);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> schema -> dataTypes\n");
    return_code = freeMemory(rel -> schema -> dataTypes);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> mgmtData\n");
    return_code = freeMemory( rel ->  mgmtData);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable rel-> schema\n");
    return_code = freeMemory(rel -> schema);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeTable: COMPLETED \n");
    return RC_OK;
}

/* Function to delete Table */
extern RC deleteTable (char *name){
    printf("deleteTable: START \n");
    FILE *file;
    int return_code = 0;
    // Checking if file exists or not. If doesn't exist return EC:
    printf("deleteTable table check \n");
    if( (file = fopen(name, "r")) == NULL )
        return RC_TABLE_DOES_NOT_EXIST;
    else
        fclose(file);
    
    printf("deleteTable for file \n");
    return_code = destroyPageFile(name);
    if( return_code != RC_OK)
        return return_code;
    
    printf("deleteTable: COMPLETED \n");
    return RC_OK;
}

/* Funtion to get number of tuples */
extern int getNumTuples (RM_TableData *rel){
    printf("getNumTuples: START \n");
    tblManagement *table_mgmt = rel-> mgmtData;
    int num_tuples = table_mgmt -> no_of_tuples;
    printf("Num tuples are : %d\n", num_tuples);
    printf("getNumTuples: Completed \n");
    return num_tuples;
}

/* Writes table data to file*/
RC persistTable(char *file_name, tblManagement *table_data) {
    printf("persistTable : START \n");
    FILE *file;
    int return_code;
    
    // Checking if file exists or not. If does not exist return EC:
    printf("persistTable file name check\n");
    if( (file = fopen(file_name, "r")) == NULL )
        return -1;
    else
        fclose(file);
    
    printf("persistTable open page file\n");
    return_code = openPageFile(file_name, &file_handle);
    if( return_code != RC_OK)
        return return_code;
    
    printf("persistTable write page file\n");
    return_code = writePage(file_handle, tableToString(table_data) );
    if( return_code != RC_OK)
        return return_code;
    
    printf("persistTable close page file\n");
    return_code = closePageFile(&file_handle);
    if( return_code != RC_OK)
        return return_code;
    
    printf("persistTable : Completed \n");
    return RC_OK;
}

/* Function to insert record */
extern RC insertRecord (RM_TableData *rel, Record *record){
    printf("insertRecord : START \n");
    int size_of_bm = sizeof(BM_PageHandle);
    int return_code = 0;
    
    BM_PageHandle *pg_handler = (BM_PageHandle*)malloc(size_of_bm);
    
    tblManagement *table_data = NULL;
    table_data =(tblManagement *)(rel->mgmtData);
    
    Schema *schm = rel -> schema;
    int page_number = table_data -> rec_last_position;
    
    printf("insertRecord page_number %d\n", page_number);
    int temp = (page_number - table_data-> rec_start_position)* table_data-> maximum_slots;
    int slt = (table_data -> no_of_tuples) - temp;
    char *page_data;
    temp = table_data -> maximum_slots;
    
    if( slt == temp ) {
        page_number = page_number+1;
        slt = 0;
    }
    
    record->id.slot = slt;
    table_data -> rec_last_position = page_number;
    record->id.page = page_number;
    
    printf("insertRecord serialize record \n");
    page_data = serializeRecord(record, schm);
    
    printf("insertRecord pin page %d\n", page_number);
    return_code = pinPage(table_data->bm, pg_handler, page_number);
    if( return_code != RC_OK)
        return return_code;
    
    temp = slt * table_data->lenght_of_slot;
    int length_page_data = (int)strlen(page_data);
    memcpy( pg_handler->data + temp, page_data, length_page_data );
    
    printf("insertRecord free memory \n");
    return_code = freeMemory(page_data);
    if( return_code != RC_OK)
        return return_code;
    
    printf("insertRecord mark dirty \n");
    return_code = markDirty(table_data->bm, pg_handler);
    if( return_code != RC_OK)
        return return_code;
    
    printf("insertRecord un pin page \n");
    return_code = unpinPage(table_data->bm, pg_handler);
    if( return_code != RC_OK)
        return return_code;
    
    printf("insertRecord force page \n");
    return_code = forcePage(table_data->bm, pg_handler);
    if( return_code != RC_OK)
        return return_code;
    table_data -> no_of_tuples = table_data-> no_of_tuples + 1 ;
    
    printf("insertRecord persist table \n");
    return_code = persistTable(rel->name, table_data);
    if( return_code != RC_OK)
        return return_code;
    
    printf("insertRecord free memory \n");
    return_code = freeMemory(pg_handler);
    if( return_code != RC_OK)
        return return_code;
    
    printf("insertRecord : Completed \n");
    return RC_OK;
}

/* Function to delete record */
extern RC deleteRecord (RM_TableData *rel, RID id){
    printf("deleteRecord : START \n");
    int rec_page_no = id.page;
    int rec_slot_no = id.slot;
    int return_code =0;
    int temp=0;
    
    tblManagement *table_data =(tblManagement*)(rel->mgmtData);
    int size_of_bm = sizeof(BM_PageHandle);
    BM_PageHandle *pg = (BM_PageHandle*)malloc(size_of_bm);
    
    printf("deleteRecord pinPage \n");
    return_code = pinPage(table_data->bm, pg, rec_page_no);
    if(return_code != RC_OK)
        return return_code;
    
    printf("deleteRecord memory copy \n");
    temp = rec_slot_no * table_data-> lenght_of_slot;
    int record_size = (int) strlen( temp + pg-> data);
    temp = rec_slot_no * table_data-> lenght_of_slot;
    memcpy( pg-> data +temp, "\0", record_size);
    
    printf("deleteRecord mark dirty \n");
    return_code = markDirty(table_data->bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    printf("deleteRecord un pin page \n");
    return_code = unpinPage(table_data-> bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    printf("deleteRecord force page \n");
    return_code = forcePage(table_data-> bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    // Reduce tuple count
    table_data-> no_of_tuples = table_data -> no_of_tuples - 1;
    // Write page data to file
    printf("deleteRecord persist table \n");
    return_code = persistTable(rel->name, table_data);
    if( return_code != RC_OK)
        return return_code;
    
    printf("deleteRecord : COMPLETED \n");
    return RC_OK;
}

/* Function to update record */
extern RC updateRecord (RM_TableData *rel, Record *record){
    printf("updateRecord : START \n");
    int size_of_bm = sizeof(BM_PageHandle);
    BM_PageHandle *pg = (BM_PageHandle*)malloc(size_of_bm);
    int rec_page_no = record -> id.page;
    int return_code =0;
    int rec_slot_no = record -> id.slot;
    
    tblManagement *table_data =(tblManagement*)(rel->mgmtData);
    
    printf("updateRecord serialization of record \n");
    char *record_string = serializeRecord(record, rel->schema);
    
    printf("updateRecord pin page \n");
    return_code = pinPage(table_data->bm, pg, rec_page_no);
    if( return_code != RC_OK)
        return RC_OK;
    
    printf("updateRecord memory copy \n");
    int temp = rec_slot_no * table_data-> lenght_of_slot;
    memcpy(pg-> data + temp, record_string, (int)strlen(record_string));
    
    printf("updateRecord mark dirty \n");
    return_code = markDirty(table_data->bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    printf("updateRecord un pin page \n");
    return_code = unpinPage(table_data-> bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    printf("updateRecord force page \n");
    return_code = forcePage(table_data-> bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    // Write page data to file
    printf("updateRecord persist table \n");
    return_code = persistTable(rel->name, table_data);
    if( return_code != RC_OK)
        return return_code;
    
    printf("updateRecord free reserved memory \n");
    return_code = freeMemory(record_string);
    if( return_code != RC_OK)
        return return_code;
    
    printf("updateRecord : Completed \n");
    return RC_OK;
}

//convert the record data into string
Record *deserializeRecord(char *record_str, RM_TableData *rel)  {
    printf("deserializeRecord : START \n");
    printf(" Data recieved %s \n", record_str);
    int SIZE_OF_RECORD = sizeof(Record);
    int SIZE_OF_CHAR = sizeof(char);
    Record *record = (Record *) malloc(SIZE_OF_RECORD);
    Schema *schema = rel-> schema;
    tblManagement *table_data = (tblManagement *) (rel-> mgmtData);
    Value *temp_value;
    int temp = SIZE_OF_CHAR * table_data -> lenght_of_slot;
    record->data = (char *)malloc(temp);
    temp = (int) strlen(record_str);
    int BASE = 10;
    char data[temp];
    strcpy(data, record_str);
    char *temp_string1, *temp_string2;
    
    temp_string1 = strtok(data,"-");
    temp_string1 = strtok (NULL,"]");
    temp_string1 = strtok (NULL,"(");
    
    for(int counter =0; counter <schema->numAttr; counter++){
        temp_string1 = strtok (NULL,":");
        if(counter == (schema->numAttr - 1)){
            temp_string1 = strtok (NULL,")");
        } else{
            temp_string1 = strtok (NULL,",");
        }
        
        /* set attribute values as per the attributes datatype */
        switch(schema->dataTypes[counter]){
            case DT_INT:
            {
                printf("deserializeRecord int %s \n", temp_string1);
                int tol = (int)strtol(temp_string1, &temp_string2, BASE);
                printf("deserializeRecord strtol \n");
                MAKE_VALUE(temp_value, DT_INT, tol);
                setAttr (record, schema, counter, temp_value);
                freeVal(temp_value);
            }
                break;
            case DT_STRING:
            {
                MAKE_STRING_VALUE(temp_value, temp_string1);
                setAttr (record, schema, counter, temp_value);
                freeVal(temp_value);
            }
                break;
            case DT_FLOAT:
            {
                float val = strtof(temp_string1, NULL);
                MAKE_VALUE(temp_value, DT_FLOAT, val);
                setAttr (record, schema, counter, temp_value);
                freeVal(temp_value);
            }
                break;
            case DT_BOOL:
            {
                bool val;
                val = (temp_string1[0] == 't') ? TRUE : FALSE;
                MAKE_VALUE(temp_value, DT_BOOL, val);
                setAttr (record, schema, counter, temp_value);
                freeVal(temp_value);
            }
                break;
        }
    }
    
    return record;
}

/* Function to update record */
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    printf("getRecord : START \n");
    int size_of_bm = sizeof(BM_PageHandle);
    BM_PageHandle *pg = (BM_PageHandle*)malloc(size_of_bm);
    
    int SIZE_OF_CHAR = sizeof(char);
    int rec_page_no =  id.page;
    int return_code =0;
    int rec_slot_no = id.slot;
    int num_tuple = 0;
    
    tblManagement *table_data =(tblManagement*)(rel->mgmtData);
    char *record_string = (char*) malloc (SIZE_OF_CHAR * table_data -> lenght_of_slot);
    record -> id.page = rec_page_no;
    
    
    printf("getRecord off set calculation \n");
    int offset = (rec_page_no - table_data -> rec_start_position);
    offset = offset * (table_data-> maximum_slots);
    offset = offset + rec_slot_no;
    num_tuple = offset + 1;
    record -> id.slot = rec_slot_no;
    if ( num_tuple > table_data -> no_of_tuples){
        return_code = freeMemory(pg);
        if( return_code != RC_OK)
            return return_code;
        
        return RC_RM_NO_MORE_TUPLES;
    }
    
    printf("getRecord pinnig page \n");
    return_code = pinPage(table_data->bm, pg, rec_page_no);
    if( return_code != RC_OK)
        return return_code;
    
    printf("getRecord memory copy  \n");
    offset = rec_slot_no * table_data -> lenght_of_slot;
    printf(" Before %s  \n  Copying %d", record_string, SIZE_OF_CHAR * table_data->lenght_of_slot);
    printf("Data %s", pg->data+offset);
    memcpy(record_string, pg->data+offset, SIZE_OF_CHAR * table_data->lenght_of_slot);
    
    printf("getRecord un pin page  \n");
    return_code = unpinPage(table_data-> bm, pg);
    if( return_code != RC_OK)
        return return_code;
    
    printf("getRecord string to record  \n");
    printf(" After%s \n ", record_string);
    Record *temp = deserializeRecord(record_string, rel);
    record-> data = temp -> data;
    
    printf("getRecord free reserved memory  \n");
    return_code = freeMemory(temp);
    if( return_code != RC_OK)
        return return_code;
    return_code = freeMemory(pg);
    if( return_code != RC_OK)
        return return_code;
    
    return RC_OK;
}

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    
    printf("startScan : START \n");
    printf("startScan checking for NULL Condition \n");
    if(rel == NULL || scan == NULL){
        printf("Input arguments are NULL\n");
        return RC_NULL_ARGUMENT;
    }
    
    // Initialize recInfo
    int SIZE_OF_RECORD = sizeof(recInformation);
    recInformation *recInfo = (recInformation *) malloc(SIZE_OF_RECORD);
    
    recInfo->srch_cond = cond;
    recInfo->current_slot = 0;
    recInfo->current_page = ((tblManagement *)rel->mgmtData)->rec_start_position;
    recInfo->number_of_slots = ((tblManagement *)rel->mgmtData)->maximum_slots;
    recInfo->number_of_pages = ((tblManagement *)rel->mgmtData)->rec_last_position;
    scan->mgmtData = (void *) recInfo;
    scan->rel = rel;
    
    printf("startScan : COMPLETED \n");
    return RC_OK;
}

extern RC closeScan (RM_ScanHandle *scan){
    
    /*printf("Begin--> closeScan\n");
     recInformation *recInfo = (recInformation *) scan->mgmtData;
     free(recInfo->srch_cond);
     free(recInfo);
     //free(scan);
     printf("End--> closeScan\n"); */
    return RC_OK;
}

extern RC next (RM_ScanHandle *scan, Record *record){
    
    printf("next : START \n");
    printf("next checking for null condition \n");
    /*if(scan == NULL || record == NULL){
     printf("Input arguments are NULL\n");
     return RC_NULL_ARGUMENT;
     }*/
    
    recInformation *recInfo;
    Value *value;
    recInfo = scan->mgmtData;
    RC status;
    
    
    record->id.slot = recInfo->current_slot;
    record->id.page = recInfo->current_page;
    
    printf("next get record \n");
    status = getRecord(scan->rel, record->id, record);
    
    if(status == RC_RM_NO_MORE_TUPLES){
        printf("next NO_TUPLES_FOUND \n");
        return RC_RM_NO_MORE_TUPLES;
    } else{
        printf("next evaluation of expression \n");
        evalExpr(record, scan->rel->schema, recInfo->srch_cond, &value);
        if (recInfo->current_slot == recInfo->number_of_slots - 1){
            recInfo->current_slot = 0;
            recInfo->current_page = recInfo->current_page +1;
        }
        else{
            recInfo->current_slot =recInfo->current_slot+1;
        }
        scan->mgmtData = recInfo;
        
        printf("====== Recursive Calling next %d", value->v.boolV);
        if(value->v.boolV != 1){
            return next(scan, record);
        }
        else{
            printf("next : COMPLETED \n");
            return RC_OK;
        }
    }
    
}

// dealing with schemas
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    
    printf("Begin--> createSchema\n");
    
    Schema *schema = malloc(sizeof(Schema));
    
    if(schema != NULL) {
        // Setting Schema if the memory allocation is not null
        printf("Initializing Schema\n");
        schema->numAttr = numAttr;
        schema->attrNames = attrNames;
        schema->dataTypes = dataTypes;
        schema->typeLength = typeLength;
        schema->keySize = keySize;
        schema->keyAttrs = keys;
    } else {
        printf("Malloc failed while allocating memory for Schema\n");
    }
    printf("End--> createSchema\n");
    return schema;
}

/* Function to free memory */
extern RC freeSchema (Schema *schema){
    
    printf("Begin--> freeSchema\n");
    int return_code = 0;
    for(int i =0; i<schema->numAttr; i++) {
        return_code = freeMemory(schema->attrNames[i]);
        if( return_code != RC_OK)
            return return_code;
    }
    return_code = freeMemory(schema->dataTypes);
    if( return_code != RC_OK)
        return return_code;
    return_code = freeMemory(schema->typeLength);
    if( return_code != RC_OK)
        return return_code;
    return_code = freeMemory(schema->keyAttrs);
    if( return_code != RC_OK)
        return return_code;
    return_code = freeMemory(schema);
    if( return_code != RC_OK)
        return return_code;
    
    printf("End--> freeSchema\n");
    return RC_OK;
}

/* Returns size of record */
int getRecordSize(Schema *schema){
    int temp_size = 0, final_size =0, counter =0;
    int SIZE_OF_INT = sizeof(int);
    int SIZE_OF_BOOL = sizeof(bool);
    int SIZE_OF_FLOAT = sizeof(float);
    if( schema -> numAttr >0 ){
        do{
            switch(schema->dataTypes[counter]){
                case DT_STRING: temp_size = schema -> typeLength[counter];
                    break;
                case DT_INT:    temp_size = SIZE_OF_INT;
                    break;
                case DT_BOOL:    temp_size = SIZE_OF_BOOL;
                    break;
                case DT_FLOAT:    temp_size = SIZE_OF_FLOAT;
                    break;
            }
            counter = counter+1;
            final_size = final_size + temp_size;
        }while( counter < schema -> numAttr);
    }
    return final_size;
}

/* Function to create record */
extern RC createRecord (Record **record, Schema *schema){
    printf("createRecord : START \n");
    int size_of_record = sizeof(record);
    *record = (Record*)malloc(size_of_record);
    
    int size = getRecordSize(schema);
    (*record) -> data = (char*) malloc(size);
    printf("createRecord : Completed \n");
    return RC_OK;
}

/* Function to free record */
extern RC freeRecord (Record *record){
    printf("freeRecord : START \n");
    int return_code = 0;
    printf("freeRecord of data \n");
    return_code = freeMemory(record -> data);
    if( return_code != RC_OK)
        return return_code;
    
    printf("freeRecord of record \n");
    return_code = freeMemory(record);
    if( return_code != RC_OK)
        return return_code;
    
    printf("freeRecord : Completed \n");
    return RC_OK;
}

/* attribute off set function */
RC attrOffset (Schema *schema, int attrNum, int *result) {
    printf("attrOffset : START \n");
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

/* Function to get attributes */
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
    printf("getAttr : START \n");
    printf("getAttr checking null \n");
    int SIZE_OF_VALUE = sizeof(Value);
    int SIZE_OF_INT = sizeof(int);
    int SIZE_OF_FLOAT = sizeof(float);
    int SIZE_OF_BOOL = sizeof(bool);
    *value = (Value *)malloc(SIZE_OF_VALUE);
    
    // Get offset value
    int offset;
    
    printf("getAttr attribute offset \n");
    attrOffset(schema, attrNum, &offset);
    
    // Set the data type
    (*value)->dt = schema->dataTypes[attrNum];
    
    // Copy value
    printf("getAttr switch case to copy values based on type \n");
    switch(schema->dataTypes[attrNum]) {
        case DT_INT:{
            memcpy(&((*value)->v.intV), record->data + offset, SIZE_OF_INT);
            break;
        }
        case DT_FLOAT:{
            memcpy(&((*value)->v.floatV), record->data + offset, SIZE_OF_FLOAT);
            break;
        }
        case DT_BOOL:{
            memcpy(&((*value)->v.boolV), record->data + offset, SIZE_OF_BOOL);
            break;
        }
        case DT_STRING: {
            int temp = schema->typeLength[attrNum] + 1;
            char *temp_string = (char *) malloc(temp);
            strncpy(temp_string, record->data + offset, temp-1);
            temp_string[temp-1] = '\0';
            (*value)->v.stringV = temp_string;
            break;
        }
    }
    
    printf("getAttr : COMPLETED \n");
    
    return RC_OK;
}

/* Function to set attributes */
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    
    printf("setAttr : START \n");
    if (value->dt != schema->dataTypes[attrNum]) {
        printf("UNKOWN DATATYPE\n");
        return RC_RM_UNKOWN_DATATYPE;
    }
    
    // Get offset value
    int offset;
    attrOffset(schema, attrNum, &offset);
    int SIZE_OF_INT = sizeof(int);
    int SIZE_OF_FLOAT = sizeof(float);
    int SIZE_OF_BOOL = sizeof(bool);
    
    // Copy value
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
            memcpy(record->data + offset, &(value->v.intV), SIZE_OF_INT);
            break;
        case DT_FLOAT:
            memcpy(record->data + offset, &(value->v.floatV), SIZE_OF_FLOAT);
            break;
        case DT_BOOL:
            memcpy(record->data + offset, &(value->v.boolV), SIZE_OF_BOOL);
            break;
        case DT_STRING:
            memcpy(record->data + offset, value->v.stringV, schema->typeLength[attrNum]);
            break;
    }
    
    printf("setAttr : COMPLETED \n");
    return RC_OK;
}
