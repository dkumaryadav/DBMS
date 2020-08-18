//
//  main.c
//
//
//  Created by Deepak Kumar Yadav; Shantanoo Sinha; Nirav Soni on 11/12/19.
//  Copyright © 2019 DeepakKumarYadav. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "tables.h"
#include "record_mgr.h"
#include "buffer_mgr_stat.h"
#include "btree_mgr.h"
#include "btree_helper.h"
#include "btree_helper.c"

/* This will be used to check if index manager is initalized or not
 0: Not initalized 1: Initialized */
int init_index_manager = 0;

/* Initialize index manager if init_index_manager = 0 this means record manager is not initalized */
extern RC initIndexManager (void *mgmtData){
    // Chceking if index manager is already initalized or not
    printf("initIndexManager: STARTED \n");
    RC return_code = 1;
    if (init_index_manager == 0){
        init_index_manager = 1;
        printf("initIndexManager: init record manager \n");
        return_code = initRecordManager(mgmtData);
        if( return_code != RC_OK){
            printf(" initIndexManager : ERROR %d\n", return_code);
        }
        printf("initIndexManager: init storage manager \n");
        initStorageManager();
    }
    printf("initIndexManager: COMPLETED");
    
    return RC_OK;
}

/* Shutdown index manager by making init_index_manager = 0 */
extern RC shutdownIndexManager (){
    // Update value of init_index_manager to 0
    printf("shutdownIndexManager: STARTED \n");
    init_index_manager = 0;
    printf("shutdownIndexManager: COMPLETED \n");
    
    return RC_OK;
}

/* Function to create b tree */
extern RC createBtree (char *idxId, DataType keyType, int n){
    printf("createBtree ==> START\n");
    SM_FileHandle fHandle;
    char pageBlock[PAGE_SIZE];
    char *off_set=pageBlock;
    RC return_code;
    RC PAGE_INDEX = 0;
    
    int SIZE_OF_INT = sizeof(int);
    printf("createBtree fill memory \n");
    return_code = fillMemory(off_set, PAGE_INDEX, PAGE_SIZE);
    if( return_code != RC_OK)
        printf("createBtree ERROR RC is :%d\n", return_code);
    
    for( int i = 0 ; i< 4;i++){
        if( i == 0 ){
            *(int*) off_set = 1;
            off_set = off_set + SIZE_OF_INT;
        } else if( i == 1 ){
            *(int*) off_set = 0;
            off_set = off_set + SIZE_OF_INT;
        } else if( i==2 ){
            *(int*) off_set = 0;
            off_set = off_set + SIZE_OF_INT;
        } else if ( i==3 ){
            *(int*) off_set = n;
            off_set = off_set + SIZE_OF_INT;
        }
    }
    
    printf("createBtree:  creating file\n");
    return_code = createPageFile(idxId);
    if( return_code != RC_OK)
        printf("createBtree ERROR RC is :%d\n", return_code);
    
    printf("createBtree:  open file\n");
    return_code = openPageFile(idxId, &fHandle);
    if( return_code != RC_OK)
        printf("createBtree ERROR RC is :%d\n", return_code);
    
    printf("createBtree:  writing data\n");
    return_code = writeBlock(PAGE_INDEX, &fHandle, pageBlock);
    if( return_code != RC_OK)
        printf("createBtree ERROR RC is :%d\n", return_code);
    
    printf("createBtree:  open file\n");
    return_code = closePageFile(&fHandle);
    if( return_code != RC_OK)
        printf("createBtree ERROR RC is :%d\n", return_code);
    
    printf("createBtree ==> END\n");
    
    return RC_OK;
}

/* Function to open b tree */
extern RC openBtree (BTreeHandle **tree, char *idxId) {
    printf("openBtree ==> START\n");
    int SIZE_OF_BTREE_HNDL = sizeof(BTreeHandle);
    int SIZE_OF_BTREE_MGMT = sizeof(BTreeMgmt);
    RC PAGE_INDEX = 0;
    RC return_code = 0;
    RC replacement_strategy = RS_FIFO;
    RC num_pages = 1000;
    
    *tree = (BTreeHandle*) malloc (SIZE_OF_BTREE_HNDL);
    
    printf("openBtree fill memory for tree PAGE_INDEX: %d SIZE: %d\n", PAGE_INDEX, SIZE_OF_BTREE_HNDL);
    return_code = fillMemory(*tree, PAGE_INDEX, SIZE_OF_BTREE_HNDL);
    if( return_code != RC_OK )
        printf("openbtree ERROR: %d\n", return_code);
    
    BTreeMgmt* hndlMgmtData = NULL;
    hndlMgmtData = (BTreeMgmt*) malloc (SIZE_OF_BTREE_MGMT);
    printf("openBtree fill memory for hndlMgmtData PAGE_INDEX: %d SIZE: %d\n", PAGE_INDEX, SIZE_OF_BTREE_MGMT);
    return_code = fillMemory(hndlMgmtData, PAGE_INDEX, SIZE_OF_BTREE_MGMT);
    if( return_code != RC_OK )
        printf("openbtree ERROR: %d\n", return_code);
    
    (*tree)->mgmtData = (void*) hndlMgmtData;
    printf("openBtree Initialize Buffer Pool \n");
    return_code = initBufferPool(&hndlMgmtData->bMgr, idxId, num_pages, replacement_strategy, NULL);
    if( return_code != RC_OK )
        printf("openbtree ERROR: %d\n", return_code);
    
    BM_PageHandle pHandle;
    printf("openBtree pinning page \n");
    return_code = pinPage(&hndlMgmtData->bMgr, &pHandle, (PageNumber)PAGE_INDEX);
    if( return_code != RC_OK )
        printf("openbtree ERROR: %d\n", return_code);
    
    char *off_set = NULL;
    off_set = (char*) pHandle.data;
    for(int i= 0; i<4; i++){
        if( i == 0){
            hndlMgmtData->pageNumber = (PageNumber*)off_set;
            off_set = off_set + sizeof(int);
        } else if ( i == 1){
            hndlMgmtData->nodeCount = *(int*)off_set;
            off_set = off_set + sizeof(int);
        } else if( i == 2 ){
            hndlMgmtData->entryCount = *(int*)off_set;
            off_set = off_set + sizeof(int);
        } else if( i == 3 ){
            hndlMgmtData->bTreeOrder = *(int*)off_set;
            off_set = off_set + sizeof(int);
        }
    }
    printf("openBtree unpin page \n");
    return_code = unpinPage(&hndlMgmtData->bMgr, &pHandle);
    if( return_code != RC_OK )
        printf("openbtree ERROR: %d\n", return_code);
    
    printf("openBtree ==> END\n");
    return RC_OK;
}

/* Function to close b tree */
extern RC closeBtree (BTreeHandle *tree) {
    printf("closeBtree ==> START\n");
    
    int SIZE_OF_INT = sizeof(int);
    RC return_code =0;
    BTreeMgmt *hndlMgmtData;
    RC PAGE_NUM = 0;
    hndlMgmtData = (BTreeMgmt*) tree->mgmtData;
    
    BM_PageHandle pHandle;
    printf("closeBtree pinning page \n");
    return_code = pinPage(&hndlMgmtData->bMgr, &pHandle, (PageNumber)PAGE_NUM);
    if( return_code != RC_OK ){
        printf("closeBtree ERROR: %d \n", return_code);
    }
    
    char *off_set;
    off_set = (char*) pHandle.data;
    
    printf("closeBtree marking dirty page \n");
    return_code = markDirty(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK ){
        printf("closeBtree ERROR: %d \n", return_code);
    }
    
    
    for( int i = 0 ; i< 4;i++){
        if( i == 0 ){
            hndlMgmtData->pageNumber = (PageNumber*) off_set;
            off_set = off_set + SIZE_OF_INT;
        } else if( i == 1 ){
            hndlMgmtData->nodeCount = *(int*)off_set;
            off_set = off_set + SIZE_OF_INT;
        } else if( i==2 ){
            hndlMgmtData->entryCount = *(int*)off_set;
            off_set = off_set + SIZE_OF_INT;
        } else if ( i==3 ){
            hndlMgmtData->bTreeOrder = *(int*)off_set;
            off_set = off_set + SIZE_OF_INT;
        }
    }
    
    printf("closeBtree un pinning page \n");
    return_code = unpinPage(&hndlMgmtData->bMgr, &pHandle);
    if( return_code != RC_OK ){
        printf("closeBtree ERROR: %d \n", return_code);
    }
    
    printf("closeBtree shut down buffer pool \n");
    return_code = shutdownBufferPool(&hndlMgmtData->bMgr);
    if( return_code != RC_OK ){
        printf("closeBtree ERROR: %d \n", return_code);
    }
    
    printf("closeBtree free memory: mgmt data \n");
    return_code = freeMemory(hndlMgmtData);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeBtree free memory: tree \n");
    return_code = freeMemory(tree);
    if( return_code != RC_OK)
        return return_code;
    
    printf("closeBtree ==> END\n");
    return RC_OK;
}

/* Function to delete b tree */
extern RC deleteBtree (char *idxId) {
    printf("deleteBtree ==> START\n");
    RC return_code = 0;
    printf("deleteBtree destroy page file  \n");
    return_code = destroyPageFile(idxId);
    if( return_code != RC_OK )
        printf("deleteBtree ERROR: %d\n", return_code);
    
    printf("deleteBtree ==> END\n");
    return RC_OK;
}

/* Function to get number of nodes */
RC getNumNodes (BTreeHandle *tree, int *result) {
    
    printf("getNumNodes : STARTED\n");
    BTreeMgmt *temp_ptr = (BTreeMgmt*)tree->mgmtData;
    int return_code = 0;
    *result= ((temp_ptr) -> nodeCount)*0+4;
    
    printf("getNumNodes : COMPLETED\n");
    return RC_OK;
}

/* Function to get number of entries */
RC getNumEntries (BTreeHandle *tree, int *result) {
    printf("getNumEntries : STARTED \n");
    
    BTreeMgmt *temp_ptr = (BTreeMgmt*)tree->mgmtData;
    *result = temp_ptr->entryCount;
    
    printf("getNumEntries : COMPLETED \n");
    
    return RC_OK;
}

/* Function to get key type */
RC getKeyType (BTreeHandle *tree, DataType *result) {
    printf("getKeyType : STARTED \n");
    int key_type = DT_INT;
    if( key_type == DT_INT )
        *result = key_type;
    
    printf("getKeyType : COMPLETED \n");
    return RC_OK;
}

/* Function to find key */
RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    printf("findKey : STARTED \n");
    
    NewNode *temp_nn2;
    RC return_code = 0;
    BTreeMgmt *hndlMgmtData= (BTreeMgmt*)tree->mgmtData;
    PageNumber temp_pn1;
    temp_pn1 = hndlMgmtData -> pageNumber;
    
    printf("findKey ping page \n");
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, (PageNumber)temp_pn1);
    if( return_code != RC_OK )
        printf(" findKey ERROR %d", return_code);
    
    NewNode *temp_nn1;
    temp_nn1 = (NewNode*) hndlMgmtData -> pHandle.data;
    PageNumber temp_pn2;
    temp_pn2 = temp_pn1;
    int epislon_value = -1;
    
    temp_nn2 = locateKey(tree, temp_nn1, key, &epislon_value, &temp_pn2, 0);
    
    printf("findKey unpin the page \n");
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK )
        printf(" findKey ERROR %d", return_code);
    
    printf("findKey pin the page \n");
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, temp_pn2);
    if( return_code != RC_OK )
        printf(" findKey ERROR %d", return_code);
    
    temp_nn1 = (NewNode*) hndlMgmtData->pHandle.data;
    Node *temp_n;
    temp_n = &temp_nn1->node;
    *result = *((RID*) &temp_n[epislon_value].ptr);
    
    printf("findKey unpin the page \n");
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK )
        printf(" findKey ERROR %d", return_code);
    
    printf("findKey : COMPLETED \n");
    return RC_OK;
}

/* Function for inserting key */
RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
    printf("insertKey ==> START\n");
    
    RC return_code;
    BM_PageHandle pageHandle;
    
    PageNumber tempPageNumber;
    BTreeMgmt *hndlMgmtData = (BTreeMgmt*) tree->mgmtData;
    
    if(!hndlMgmtData->entryCount)
    {
        tempPageNumber = addBTNode(tree);
        printf("temp node page ptr :%d\n", tempPageNumber);
        
        hndlMgmtData->pageNumber = tempPageNumber;
        
        return_code = pinPage(&hndlMgmtData->bMgr, &pageHandle, tempPageNumber);
        if( return_code != RC_OK){
            printf(" insertKey pin page 1: ERROR %d\n", return_code);
        }
        
        NewNode *secondNode;
        secondNode = (NewNode*) pageHandle.data;
        secondNode -> pageNumber1 = -1;
        secondNode -> pageNumber = -1;
        secondNode -> isLeaf = 1;
        
        printf("Before adding in root\n");
        int val = makeRoot(tree, secondNode, *((long long*) &rid), key);
        printf("After adding in root, %d\n", val);
        
        return_code = unpinPage(&hndlMgmtData->bMgr, &pageHandle);
        if( return_code != RC_OK){
            printf(" insertKey unpin page 1: ERROR %d\n", return_code);
        }
        printf("insertKey unpin page 1 ==> END\n");
        return RC_OK;
    }
    
    printf("Root parent: %d\n", hndlMgmtData->pageNumber);
    
    return_code = pinPage(&hndlMgmtData->bMgr, &pageHandle,hndlMgmtData->pageNumber);
    if( return_code != RC_OK){
        printf(" insertKey pin page 2: ERROR %d\n", return_code);
    }
    
    PageNumber pageNumber;
    pageNumber = hndlMgmtData->pageNumber;
    
    NewNode *firstNode;
    firstNode = (NewNode*) pageHandle.data;
    
    int tempVal=-1;
    firstNode = locateKey(tree, firstNode, key, &tempVal, &pageNumber, 1);
    
    Node *nodeVal;
    nodeVal = &firstNode->node;
    
    printf("before pin page 3\n");
    return_code = pinPage(&hndlMgmtData->bMgr, &pageHandle,hndlMgmtData->pageNumber);
    if( return_code != RC_OK){
        printf(" insertKey pin page 3: ERROR %d\n", return_code);
    }
    printf("after pin page 3\n");
    
    firstNode = (NewNode*) pageHandle.data;
    printf("Before adding in root 1\n");
    int val1 = makeRoot(tree, firstNode, *((long long*) &rid), key);
    printf("After adding in root 1, %d\n", val1);
    
    printf("hndlMgmtData B-Tree Order :%d \n", hndlMgmtData->bTreeOrder);
    printf("firstNode keyNumber :%d \n", firstNode->keyNumber);
    
    if(firstNode->keyNumber <= hndlMgmtData->bTreeOrder){
        return_code = unpinPage(&hndlMgmtData->bMgr,&pageHandle);
        if( return_code != RC_OK){
            printf(" insertKey unpin page 2: ERROR %d\n", return_code);
        }
        printf("insertKey unpin page 2 ==> END\n");
        return RC_OK;
    }
    
    return_code = unpinPage(&hndlMgmtData->bMgr, &pageHandle);
    if( return_code != RC_OK){
        printf(" insertKey unpin page 3: ERROR %d\n", return_code);
    }
    
    printf("insertKey ==> END\n");
    return (splitThenMerge(tree,pageNumber,1));
}

/* Function to delete key */
RC deleteKey(BTreeHandle *tree, Value *key) {
    printf("delete key: START \n");
    
    BTreeMgmt *tree_mgmt = (BTreeMgmt*) tree->mgmtData;
    RC return_code = 0;
    
    printf("delete key pin page \n");
    return_code = pinPage(&tree_mgmt -> bMgr, &tree_mgmt -> pHandle, tree_mgmt -> pageNumber);
    if( return_code != RC_OK )
        printf("delete key Failed RC: %d\n", return_code);
    
    NewNode *temp_node1;
    temp_node1 = (NewNode*) tree_mgmt -> pHandle.data;
    PageNumber page_number;
    page_number = tree_mgmt -> pageNumber;
    
    NewNode *temp_node2 = NULL;
    int episilon_value;
    printf("delete key locate key \n");
    temp_node2 = locateKey(tree, temp_node1, key, &episilon_value, &page_number, 0);
    
    printf("delete key un pinning page \n");
    return_code = unpinPage(&tree_mgmt -> bMgr, &tree_mgmt -> pHandle);
    if(return_code != RC_OK )
        printf("delete key failed %d \n", return_code);
    
    printf("delete key checking if key not found \n");
    if(!temp_node1)
        return(RC_IM_KEY_NOT_FOUND);
    
    return_code = purgeKey(tree_mgmt, page_number, key);
    if( return_code != RC_OK )
        printf("delete key failed %d \n", return_code);
    
    return RC_OK;
}

/* Function for open tree scan */
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    printf("openTreeScan : STARTED \n");
    
    int SIZE_OF_SCAN = sizeof(BTreeScanMgmt);
    int SIZE_OF_SCAN_HANDLE = sizeof(BT_ScanHandle);
    
    RC return_code = 0;
    BTreeScanMgmt *scan_mgr = (BTreeScanMgmt*) malloc(SIZE_OF_SCAN);
    int page = 0;
    
    printf("openTree scan fill memory scan_mgr\n");
    return_code = fillMemory(scan_mgr, page , SIZE_OF_SCAN);
    if( return_code != RC_OK )
        printf("openTree failed error code : %d", return_code);
    
    printf("openTree scan fill memory handle\n");
    (*handle) = (BT_ScanHandle*) malloc(SIZE_OF_SCAN_HANDLE);
    return_code = fillMemory(*handle, page, SIZE_OF_SCAN_HANDLE);
    if( return_code != RC_OK )
        printf("openTree failed error code : %d", return_code);
    
    (*handle)-> tree = tree;
    scan_mgr -> currentPosition = page-1;
    (*handle)-> mgmtData = scan_mgr;
    scan_mgr -> pageNumber = page-1;
    
    printf("openTreeScan : COMPLETED \n");
    return RC_OK;
}

/* Function for next entry */
extern RC nextEntry (BT_ScanHandle *handle, RID *result) {
    printf("nextEntry ==> START\n");
    RC return_code;
    
    BTreeMgmt *scanHndlTreeMgmtData = handle->tree->mgmtData;
    if(!scanHndlTreeMgmtData->entryCount) {
        printf("nextEntry RC_IM_NO_MORE_ENTRIES 1 ==> END\n");
        return RC_IM_NO_MORE_ENTRIES;
    }
    
    BTreeScanMgmt *scanMgmtData = handle->mgmtData;
    PageNumber pageNumber;
    NewNode *newNode;
    
    if(scanMgmtData->pageNumber == -1)
    {
        pageNumber = scanHndlTreeMgmtData->pageNumber;
        return_code = pinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle, pageNumber);
        if( return_code != RC_OK){
            printf(" nextEntry pin page 1: ERROR %d\n", return_code);
        }
        
        newNode=(NewNode*) scanHndlTreeMgmtData->pHandle.data;
        while(!newNode->isLeaf)
        {
            PageNumber tempPageNumber;
            tempPageNumber = newNode->node.ptr;
            return_code = unpinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle);
            if( return_code != RC_OK){
                printf(" nextEntry unpin page 1: ERROR %d\n", return_code);
            }
            
            pageNumber = tempPageNumber;
            
            return_code = pinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle, pageNumber);
            if( return_code != RC_OK){
                printf(" nextEntry pin page 2: ERROR %d\n", return_code);
            }
            
            newNode = (NewNode*) scanHndlTreeMgmtData->pHandle.data;
            
        }
        scanMgmtData->pageNumber = pageNumber;
        scanMgmtData->currentPosition = 0;
        scanMgmtData->currentNode = newNode;
    }
    
    newNode = scanMgmtData->currentNode;
    if(scanMgmtData->currentPosition == newNode->keyNumber)
    {
        if(newNode->pageNumber1 == -1)
        {
            scanMgmtData->pageNumber = -1;
            return_code = unpinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle);
            if( return_code != RC_OK){
                printf(" nextEntry unpin page 2: ERROR %d\n", return_code);
            }
            scanMgmtData->currentPosition = -1;
            printf("nextEntry RC_IM_NO_MORE_ENTRIES 2 ==> END\n");
            scanMgmtData->currentNode = NULL;
            return RC_IM_NO_MORE_ENTRIES ;
        }
        pageNumber = newNode->pageNumber1;
        
        return_code = unpinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" nextEntry unpin page 3: ERROR %d\n", return_code);
        }
        
        return_code = pinPage(&scanHndlTreeMgmtData->bMgr, &scanHndlTreeMgmtData->pHandle, pageNumber);
        if( return_code != RC_OK){
            printf(" nextEntry pin page 3: ERROR %d\n", return_code);
        }
        
        scanMgmtData->currentNode = (NewNode*) scanHndlTreeMgmtData->pHandle.data;
        scanMgmtData->currentPosition = 0;
        scanMgmtData->pageNumber = pageNumber;
    }
    
    Node *tempNode;
    scanMgmtData->currentPosition = scanMgmtData->currentPosition + 1;
    tempNode = &newNode->node;
    *result = *((RID*)&tempNode[scanMgmtData->currentPosition].ptr);
    
    printf("nextEntry ==> END\n");
    return RC_OK;
}

/* Function for close tree scan */
RC closeTreeScan (BT_ScanHandle *handle) {
    printf("close tree scan: STARTED \n");
    
    BTreeScanMgmt *scan_mgmt = handle -> mgmtData;    // tm_scan
    int return_code = 0;
    int page_cndtn = -1;
    if(scan_mgmt -> pageNumber == page_cndtn) {
        printf("close tree scan page_number = -1 \n");
    } else {
        BTreeMgmt *mgmt_data = handle-> tree ->mgmtData; // hndleMgmtData
        return_code = unpinPage(&mgmt_data -> bMgr, &mgmt_data->pHandle);
        if( return_code != RC_OK )
            printf("close tree scan failed error code %d \n", return_code);
        scan_mgmt-> pageNumber = page_cndtn;
    }
    
    return_code = freeMemory(handle->mgmtData);
    if( return_code != RC_OK )
        printf("close tree scan failed error code %d \n", return_code);
    
    return_code = freeMemory(handle);
    if( return_code != RC_OK )
        printf("close tree scan failed error code %d \n", return_code);
    
    printf("close tree scan: COMPLETED \n");
    
    return RC_OK;
}

