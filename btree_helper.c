//
//  btree_helper.c
//  RecordManager
//
//  Created by Deepak Kumar Yadav on 11/18/19.
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


/* Function for creating new b tree node */
static int addBTNode(BTreeHandle *tree) {
    printf("addBTNode ==> START\n");
    BTreeMgmt *hndlMgmtData = (BTreeMgmt*) (tree->mgmtData);
    BM_mgmtinfo* bmMgmtInfo= (BM_mgmtinfo *)hndlMgmtData->bMgr.mgmtData;
    hndlMgmtData->nodeCount = hndlMgmtData->nodeCount + 1;
    printf("Total Node Count :%d\n", hndlMgmtData->nodeCount);
    printf("B-Tree Order level :%d\n", hndlMgmtData->bTreeOrder);
    printf("addBTNode ==> END\n");
    return bmMgmtInfo->fh->totalNumPages-1;
}

/* Function to fill a block of memory */
int fillMemory(void *pointer, int value, int size){
    printf("fillMemory ==> START\n");
    memset(pointer, value, size);
    printf("fillMemory ==> COMPLETED\n");
    return RC_OK;
}

/* Function to move a block of memory */
int moveMemory(void *destination, void *source, int size){
    printf("moveMemory ==> START\n");
    memmove(destination, source, size);
    printf("moveMemory ==> COMPLETED\n");
    return RC_OK;
}

/* Function to locate key */
RC locateKey (BTreeHandle *tree, NewNode *temp_nn1, Value *key, int *epislon_value, int *temp_pn2, bool matched){
    printf("Locate Key: START \n");
    
    BM_PageHandle page_handler ;
    
    int greater;
    NewNode *temp_new_node; // n: temp_new_node
    PageNumber temp_pg_no; // pn1: temp_pg_no
    
    BTreeMgmt *tree_mgmt = (BTreeMgmt*)tree->mgmtData; // tm: tree_mgmt
    RC return_code = 0;
    
    printf("Locate Key: checking for key \n");
    if( ! temp_nn1 -> keyNumber )
        return NULL;
    
    Node *temp_node;
    temp_node = &temp_nn1 -> node;
    
    int counter = 0;
    
    while(counter < temp_nn1 -> keyNumber) {
        
        // Value smaller check
        printf("Locate Key value smaller check \n");
        Value result;
        valueSmaller(&temp_node[counter].key, key, &result);
        
        // Value equals
        printf("Locate Key value equals check \n");
        Value equal;
        valueEquals(&temp_node[counter].key, key, &equal);
        
        greater = !result.v.boolV && !equal.v.boolV;
        
        if(temp_nn1 -> isLeaf) {
            if(equal.v.boolV) {
                *epislon_value = counter;
                return temp_nn1;
            }
            if(greater) {
                if(matched) {
                    *epislon_value = counter;
                    return temp_nn1;
                }
                *epislon_value = -1;
                *temp_pn2 = -1;
                return NULL;
            }
        } else if(greater) {
            PageNumber page_no = (PageNumber) temp_node[counter].ptr;
            return_code = pinPage(&tree_mgmt->bMgr, &page_handler, page_no);
            if( return_code != RC_OK){
                printf("Locate Key failed ERROR CODE %d\n", return_code);
            }
            temp_new_node = (NewNode*)page_handler.data;
            *temp_pn2 = page_no;
            temp_nn1 = locateKey(tree, temp_new_node, key, epislon_value, temp_pn2, matched);
            return_code = unpinPage(&tree_mgmt->bMgr, &page_handler);
            if( return_code != RC_OK){
                printf("Locate Key failed ERROR CODE %d\n", return_code);
            }
            
            return(temp_nn1);
        }
        counter++;
    }
    
    if(temp_nn1-> isLeaf) {
        if(matched) {
            *epislon_value = counter;
            return(temp_nn1);
        }
        *epislon_value = *temp_pn2 = -1;
        return(NULL);
    }
    
    temp_pg_no = temp_nn1 -> pageNumber1;
    
    return_code = pinPage(&tree_mgmt->bMgr, &page_handler, temp_pg_no);
    if( return_code != RC_OK)
        printf("Locate Key failed ERROR %d\n", return_code);
    
    temp_new_node = (NewNode*)page_handler.data;
    *temp_pn2 = temp_pg_no;
    printf(" Locate Key : Calling search key again \n");
    temp_nn1 = locateKey(tree, temp_new_node, key, epislon_value, temp_pn2, matched);
    return_code = unpinPage(&tree_mgmt->bMgr, &page_handler);
    if( return_code != RC_OK)
        printf("Locate Key failed ERROR %d\n", return_code);
    
    printf("Locate Key: COMPLETED \n");
    
    return(temp_nn1);
}

/* Function to put key in root */
static int makeRoot(BTreeHandle *tree, NewNode *newNode1, long long ptr, Value *key ) {
    
    printf("makeRoot ==> START\n");
    RC return_code;
    int SIZE_OF_NODE = sizeof(Node);
    
    Node *node;
    node = &newNode1->node;
    
    int counter = 0;
    Value value;
    while(counter < newNode1->keyNumber){
        valueSmaller(&node[counter].key, key, &value);
        if(!value.v.boolV){
            break;
        }
        counter = counter + 1;
    }
    {
        return_code = moveMemory((char*) &node[counter+1], (char*)&node[counter], (int) (newNode1->keyNumber-counter+1)*SIZE_OF_NODE);
        if( return_code != RC_OK )
            printf(" makeRoot ERROR %d", return_code);
    }
    
    node[counter].key = *key;
    node[counter].ptr = ptr;
    
    BTreeMgmt *hndlMgmtData=(BTreeMgmt*) tree->mgmtData;
    if(newNode1->isLeaf){
        hndlMgmtData->entryCount = hndlMgmtData->entryCount + 1;
    }
    
    printf("makeRoot: Node key num before: %d \n",newNode1->keyNumber);
    newNode1->keyNumber = newNode1->keyNumber + 1;
    printf("makeRoot: Node key num after : %d \n",newNode1->keyNumber);
    printf("makeRoot ==> END\n");
    return counter;
}

/* Function to add key node to root */
static RC updateRoot(BTreeHandle *tree, PageNumber pagNum1, PageNumber pagNum2, Value key) {
    printf("updateRoot ==> START\n");
    RC return_code;
    
    BTreeMgmt *hndlMgmtData = (BTreeMgmt*)tree->mgmtData;
    return_code= pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, pagNum1);
    if( return_code != RC_OK){
        printf(" updateRoot pin page 1: ERROR %d\n", return_code);
    }
    
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, pagNum2);
    if( return_code != RC_OK){
        printf(" updateRoot pin page 2: ERROR %d\n", return_code);
    }
    
    NewNode *new_node1;
    PageNumber tempPageNum;
    NewNode *new_node2;
    
    if(new_node2->pageNumber == -1){
        tempPageNum = addBTNode(tree);
        hndlMgmtData->pageNumber = tempPageNum;
        
        return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, tempPageNum);
        if( return_code != RC_OK){
            printf(" updateRoot pin page 3: ERROR %d\n", return_code);
        }
        new_node1 = (NewNode*) ((hndlMgmtData->pHandle).data);
        new_node1->pageNumber = -1;
        new_node1->pageNumber1 = -1;
        new_node1->isLeaf = 0;
    } else {
        tempPageNum = new_node2->pageNumber;
        return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, tempPageNum);
        if( return_code != RC_OK){
            printf(" updateRoot pin page 4: ERROR %d\n", return_code);
        }
        new_node1 = (NewNode*) ((hndlMgmtData->pHandle).data);
    }
    
    NewNode *new_node3;
    Node *node1;
    if(new_node2->isLeaf){
        node1 = &new_node3->node;
        key = node1[0].key;
    }
    
    int counter = 0;
    counter = makeRoot(tree, new_node1, pagNum1, &key);
    
    int keyNum;
    keyNum = new_node1->keyNumber;
    
    if(counter == keyNum-1){
        new_node1->pageNumber1 = pagNum2;
    }else{
        node1 = &new_node1->node;
        node1[counter+1].ptr = pagNum2;
    }
    
    new_node2->pageNumber = tempPageNum;
    new_node3->pageNumber = tempPageNum;
    
    (&hndlMgmtData->pHandle)->pageNum = tempPageNum;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" updateRoot unpin page 1: ERROR %d\n", return_code);
    }
    
    (&hndlMgmtData->pHandle)->pageNum = pagNum1;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" updateRoot unpin page 2: ERROR %d\n", return_code);
    }
    
    (&hndlMgmtData->pHandle)->pageNum = pagNum2;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" updateRoot unpin page 3: ERROR %d\n", return_code);
    }
    
    if(keyNum <= hndlMgmtData->bTreeOrder){
        (&hndlMgmtData->pHandle)->pageNum = tempPageNum;
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" updateRoot unpin page 4: ERROR %d\n", return_code);
        }
        printf("updateRoot check btreeorder==> END\n");
        return RC_OK;
    }
    
    (&hndlMgmtData->pHandle)->pageNum = tempPageNum;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" updateRoot unpin page 5: ERROR %d\n", return_code);
    }
    
    printf("updateRoot ==> END\n");
    return(splitThenMerge(tree,tempPageNum,0));
}

/* Function to divide and add node */
static RC splitThenMerge(BTreeHandle *tree, PageNumber pageN, bool isLeaf) {
    printf("splitThenMerge ==> START\n");
    RC return_code;
    int SIZE_OF_NODE = sizeof(Node);
    
    BTreeMgmt *hndlMgmtData=(BTreeMgmt*)tree->mgmtData;
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, pageN);
    if( return_code != RC_OK){
        printf(" splitThenMerge pin page 1: ERROR %d\n", return_code);
    }
    
    NewNode *newNode1;
    newNode1 = (NewNode*) ((hndlMgmtData->pHandle).data);
    
    Node *node2;
    node2 = &newNode1->node;
    
    PageNumber pageNum1;
    pageNum1=addBTNode(tree);
    
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, pageNum1);
    if( return_code != RC_OK){
        printf(" splitThenMerge pin page 2: ERROR %d\n", return_code);
    }
    
    NewNode *newNode2;
    newNode2=(NewNode*)((hndlMgmtData->pHandle).data);
    newNode2->pageNumber=newNode1->pageNumber;
    newNode2->isLeaf=isLeaf;
    
    Node *node1;
    node1 = &newNode2->node;
    
    int number;
    number=(hndlMgmtData->bTreeOrder+1)/2;
    newNode2->pageNumber1=newNode1->pageNumber1;
    
    PageNumber pageNum2;
    Value val;
    int temp = 0;
    if(!isLeaf){
        temp=1;
        
        val = node2[newNode1->keyNumber-number-1].key;
        pageNum2 = node2[newNode1->keyNumber-number-1].ptr;
        
        newNode1->pageNumber1 = pageNum2;
    }
    else{
        newNode1->pageNumber1 = pageNum1;
    }
    
    char *charVal1, *charVal2;
    charVal1 = (char*) &node1[0];
    charVal2 = (char*) &node2[newNode1->keyNumber-number];
    
    newNode2->keyNumber=number;
    return_code = memoryCopy(charVal1, charVal2, newNode2->keyNumber*SIZE_OF_NODE);
    if( return_code != RC_OK){
        printf(" splitThenMerge memcpy: ERROR %d\n", return_code);
    }
    newNode1->keyNumber=newNode1->keyNumber-number+temp;
    
    (&hndlMgmtData->pHandle)->pageNum = pageN;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" splitThenMerge unpin page 1: ERROR %d\n", return_code);
    }
    
    (&hndlMgmtData->pHandle)->pageNum = pageNum1;
    return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
    if( return_code != RC_OK){
        printf(" splitThenMerge unpin page 2: ERROR %d\n", return_code);
    }
    printf("splitThenMerge ==> END\n");
    return(updateRoot(tree, pageN, pageNum1, val));
}

/* Function to deleteNode */
static RC deleteNode(BTreeHandle *tree, NewNode *new_node1, PageNumber pageNumber, Value *key ) {
    
    printf("deleteNode STARTED\n");
    BTreeMgmt *tree_mgmt = (BTreeMgmt*) tree->mgmtData;
    Node *node1;
    node1 = &new_node1->node;
    int return_code = 0;
    int counter = 0;
    int SIZE_OF_NODE = sizeof(Node);
    
    printf("deleteNode checking if oprtn needs to be done\n");
    if ( new_node1 -> keyNumber > 0){
        do{
            Value *result;
            if(new_node1->isLeaf) {
                printf("deleteNode is leaf check is TRUE\n");
                valueEquals(&node1[counter].key,key, &result);
                if(result->v.boolV) {
                    if(counter<new_node1->keyNumber-1){
                        int temp = 0;
                        temp = new_node1 -> keyNumber;
                        temp = temp - counter;
                        temp = temp + 1;
                        temp = temp * SIZE_OF_NODE;
                        printf("deleteNode memory move\n");
                        return_code = moveMemory((char*) &node1[counter], (char*) &node1[counter+1], temp);
                        if( return_code != RC_OK)
                            printf("deleteNode failed %d", return_code);
                    }
                    printf("deleteNode updating keyNum and entry counter\n");
                    int temp = 0;
                    temp = new_node1 -> keyNumber;
                    temp = temp -1;
                    new_node1->keyNumber = temp;
                    temp = tree_mgmt -> entryCount;
                    temp = temp-1;
                    tree_mgmt->entryCount = temp;
                    return counter;
                }
            } else {
                printf("deleteNode is leaf check is FALSE\n");
                valueEquals(&node1[counter].key,key, &result);
                if(result->v.boolV||node1[counter].ptr==pageNumber) {
                    PageNumber page_num;
                    if(counter==0)
                        page_num = node1[0].ptr;
                    int temp = 0;
                    temp = new_node1 -> keyNumber;
                    temp = temp - counter;
                    temp = temp +1;
                    temp = temp * SIZE_OF_NODE;
                    printf("deleteNode move memory\n");
                    return_code = moveMemory((char*) &node1[counter], (char*) &node1[counter+1], temp);
                    if( return_code != RC_OK)
                                               printf("deleteNode failed %d", return_code);
                    if(counter==0)
                        node1[0].ptr= page_num;
                    printf("deleteNode updating key num\n");
                    temp = new_node1 -> keyNumber;
                    temp = temp -1;
                    new_node1->keyNumber = temp;
                    printf("deleteNode updating entry count \n");
                    temp = tree_mgmt -> entryCount;
                    temp = temp - 1;
                    tree_mgmt->entryCount = temp;
                    
                    return counter;
                }
                int temp1  = new_node1->keyNumber;
                int temp2  = new_node1->pageNumber1;
                if(counter+1 == temp1 && temp2 == pageNumber) {
                    int oprtn = 0;
                    oprtn = new_node1 -> keyNumber;
                    oprtn = oprtn -1;
                    new_node1->keyNumber = oprtn;
                    oprtn = tree_mgmt -> entryCount;
                    oprtn = oprtn -1;
                    tree_mgmt->entryCount = oprtn;
                    oprtn = counter + 1;
                    return oprtn;
                }
            }
            counter++;
        }while(counter < new_node1->keyNumber);
    }
    printf("deleteNode COMPLETED\n");
    return RC_OK;
}

/* Function to join keys */
static RC mergeKeys(BTreeHandle *tree, PageNumber page1, PageNumber page2) {
    
    printf("mergeKeys ==> START\n");
    RC return_code;
    int SIZE_OF_NODE = (int) sizeof(Node);
    
    bool isPage1LessThanZero = (page1 < 0);
    if(isPage1LessThanZero)
        page1 = page1*-1;
    // else
    //     page1 = page1;
    
    BTreeMgmt *hndlMgmtData = (BTreeMgmt*) tree->mgmtData;
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, page1);
    if( return_code != RC_OK){
        printf(" mergeKeys pin page 1: ERROR %d\n", return_code);
    }
    
    NewNode *left;
    left = (NewNode*) &hndlMgmtData->pHandle.data;
    
    return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, page2);
    if( return_code != RC_OK){
        printf(" mergeKeys pin page 2: ERROR %d\n", return_code);
    }
    
    NewNode *right;
    Node *rightNode;
    rightNode = &right->node;
    right = (NewNode*) &hndlMgmtData->pHandle.data;
    
    Node *leftNode;
    leftNode = &left->node;
    
    int mergePointer;
    Value mergeKey;
    
    if(left->isLeaf) {
        mergePointer=right->pageNumber;
        mergeKey = rightNode[0].key;
        
        return_code = memoryCopy((char*)&leftNode[left->keyNumber], (char*) &rightNode[0], SIZE_OF_NODE);
        if( return_code != RC_OK)
            printf("mergeKeys ERROR RC is :%d\n", return_code);
        
        if(isPage1LessThanZero){
            char *z = (char*) malloc(SIZE_OF_NODE*left->keyNumber);
            return_code = memoryCopy(z, (char*) &leftNode[0], SIZE_OF_NODE*left->keyNumber);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
            
            return_code = memoryCopy((char*)&leftNode[0], (char*) &rightNode[0], SIZE_OF_NODE);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
            
            return_code = memoryCopy((char*)&leftNode[right->keyNumber], (char*) z, SIZE_OF_NODE*left->keyNumber);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
        }
        else {
            left->pageNumber1 = right->pageNumber1;
        }
        
        left->keyNumber = left->keyNumber + right->keyNumber;
        
        return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, right->pageNumber);
        if( return_code != RC_OK){
            printf(" mergeKeys pin page 3: ERROR %d\n", return_code);
        }
        
        NewNode *hndlData;
        hndlData = (NewNode*) hndlMgmtData->pHandle.data;
        if(hndlData->pageNumber1 == page2) {
            hndlData->pageNumber1 = page1;
        }
        
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 1: ERROR %d\n", return_code);
        }
        
        right->keyNumber = 0;
        hndlMgmtData->nodeCount = hndlMgmtData->nodeCount - 1;
        
        (&hndlMgmtData->pHandle)->pageNum = page1;
        
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 2: ERROR %d\n", return_code);
        }
        
        (&hndlMgmtData->pHandle)->pageNum = page2;
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 3: ERROR %d\n", return_code);
        }
        printf("mergeKeys if ==> END\n");
        return purgeKey(tree, mergePointer, &mergeKey);
    } else if((left->keyNumber+right->keyNumber) < hndlMgmtData->bTreeOrder) {
        return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, right->pageNumber);
        if( return_code != RC_OK){
            printf(" mergeKeys pin page 4: ERROR %d\n", return_code);
        }
        
        NewNode *page2;
        page2= (NewNode*) hndlMgmtData->pHandle.data;
        
        Node *node1;
        node1=&page2->node;
        
        int counter = 0;
        int index;
        while(counter < page2->keyNumber) {
            if(node1[counter].ptr == page2){
                index = counter;
                break;
            }
            counter = counter + 1;
        }
        
        leftNode[left->keyNumber].ptr = left->pageNumber1;
        leftNode[left->keyNumber].key = node1[index].key;
        left->keyNumber = left->keyNumber + 1;
        
        return_code = memoryCopy((char*) &leftNode[left->keyNumber], (char*) &rightNode[0], (int) right->keyNumber*SIZE_OF_NODE);
        if( return_code != RC_OK)
            printf("mergeKeys ERROR RC is :%d\n", return_code);
        
        if(isPage1LessThanZero){
            char *tmp=(char*) malloc(SIZE_OF_NODE*left->keyNumber);
            
            return_code = memoryCopy(tmp, (char*) &leftNode[0], SIZE_OF_NODE*left->keyNumber);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
            
            return_code = memoryCopy((char*)&leftNode[0], (char*) &rightNode[0], SIZE_OF_NODE);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
            
            return_code = memoryCopy((char*)&leftNode[right->keyNumber], (char*) tmp, SIZE_OF_NODE*left->keyNumber);
            if( return_code != RC_OK)
                printf("mergeKeys ERROR RC is :%d\n", return_code);
        } else {
            left->pageNumber1 = right->pageNumber1;
        }
        
        mergePointer=right->pageNumber;
        mergeKey=rightNode[0].key;
        
        left->keyNumber = left->keyNumber + right->keyNumber;
        right->keyNumber = 0;
        hndlMgmtData->nodeCount = hndlMgmtData->nodeCount - 1;
        
        int counter1 = 0;
        NewNode *tmp;
        while (counter1 < left->keyNumber) {
            return_code = pinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle, leftNode[counter1].ptr);
            if( return_code != RC_OK){
                printf(" mergeKeys pin page 5: ERROR %d\n", return_code);
            }
            tmp->pageNumber = page1;
            return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
            if( return_code != RC_OK){
                printf(" mergeKeys unpin page 4: ERROR %d\n", return_code);
            }
            counter1 = counter1 + 1;
        }
        
        (&hndlMgmtData->pHandle)->pageNum = right->pageNumber;
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 5: ERROR %d\n", return_code);
        }
        
        (&hndlMgmtData->pHandle)->pageNum = page1;
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 6: ERROR %d\n", return_code);
        }
        
        (&hndlMgmtData->pHandle)->pageNum = page2;
        return_code = unpinPage(&hndlMgmtData->bMgr, &hndlMgmtData->pHandle);
        if( return_code != RC_OK){
            printf(" mergeKeys unpin page 7: ERROR %d\n", return_code);
        }
        
        printf("mergeKeys elseif ==> END\n");
        return purgeKey(tree, mergePointer, &mergeKey);
        
    }
    printf("mergeKeys ==> END\n");
    return RC_OK;
}

/* Function to copy memory */
int memoryCopy( void * pointer1, void *pointer2, int size){
    printf("Memory copy started \n");
    memcpy(pointer1, pointer2, size);
    printf("Memory copy completed \n");
    return RC_OK;
}

/* Function to merge keys */
static RC distributeKeys(BTreeHandle *tree, PageNumber page_number, PageNumber pageNumber){
    printf("Spread key : STARTED \n");
    
    RC return_code = 0;
    int neg_one = -1;
    
    BTreeMgmt *tree_mgmt=(BTreeMgmt*) tree->mgmtData;
    
    bool bool_val = (page_number < 0);
    printf("Spread key initalize page_number \n");
    if( !bool_val )
        page_number = page_number;
    else
        page_number = page_number * neg_one;
    
    printf("Spread key initalize pinning page page_number \n");
    return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, page_number);
    if(return_code != RC_OK )
        printf("spreakKeys failed error code %d\n", return_code);
    
    NewNode *new_node_lt;
    new_node_lt = (NewNode*)&tree_mgmt -> pHandle.data;
    printf("Spread key initalize pinning page new_node_lt \n");
    return_code = pinPage(&tree_mgmt -> bMgr, &tree_mgmt -> pHandle, pageNumber);
    if(return_code != RC_OK )
        printf("spreakKeys failed error code %d\n", return_code);
    
    NewNode *new_node_rt;
    new_node_rt = (NewNode*)&tree_mgmt->pHandle.data;
    
    Node *node_lt;
    node_lt = &new_node_lt -> node;
    
    Node *node_rt;
    node_rt = &new_node_rt->node;
    
    bool bool_chck = new_node_lt -> isLeaf;
    if(! bool_chck ) {
        printf("Spead keys no action as new_node_lt is not leaf \n");
    } else {
        int mid_point;
        Value mid_key;
        mid_key = node_rt[0].key;
        mid_point = new_node_rt -> pageNumber;
        
        printf("Spead keys memory copy \n");
        int SIZE_OF_NODE = (int) sizeof(Node);
        return_code = memoryCopy( (char*)&node_lt[new_node_lt->keyNumber], (char*) &node_rt[0], SIZE_OF_NODE);
        if( return_code != RC_OK )
            printf("distributeKeys failed error code %d ", return_code);
        
        bool purge_check = true;
        if( ! bool_val) {
            printf("Spead keys bool_val is not true update pageNumber1 \n");
            new_node_lt -> pageNumber1 = new_node_rt -> pageNumber1;
            if( purge_check != true )
                purge_check = true;
        } else{
            printf("Spead keys bool_val is true perform memcopy from lt to lt_0\n");
            int temp = new_node_lt -> keyNumber * SIZE_OF_NODE;
            printf("Spead keys bool_val is  true perform memcopy \n");
            
            for( int i = 0 ;i<3 ;i++){
                if( i == 0 ){
                    return_code = memoryCopy((char*) malloc( new_node_lt -> keyNumber*SIZE_OF_NODE),
                    (char*) &node_lt[0],
                    temp);
                } else if( i == 1) {
                    printf("Spead keys bool_val is  true perform memcopy from lt_0 to rt_0\n");
                               temp = SIZE_OF_NODE;
                               return_code = memoryCopy((char*) &node_lt[0],
                                                        (char*) &node_rt[0],
                                                        temp);
                } else if( i == 2 ){
                    printf("Spead keys bool_val is  true perform memcopy from lt_1 to lt \n");
                    temp = new_node_lt->keyNumber*SIZE_OF_NODE;
                    return_code = memoryCopy((char*) &node_lt[1],
                                             (char*) malloc( new_node_lt -> keyNumber*SIZE_OF_NODE ),
                                             temp );
                }
            }
            purge_check = true;
        }
        
        printf("Spead keys update left count \n");
        int temp = 0;
        temp = new_node_lt -> keyNumber;
        temp = temp +1;
        new_node_lt->keyNumber = temp ;
        if( purge_check == true)
            purge_check = false;
        
        printf("Spead keys update right count \n");
        temp = new_node_rt -> keyNumber;
        temp = temp + 1;
        new_node_rt->keyNumber = temp;
        
        (&tree_mgmt->pHandle) -> pageNum = page_number;
        
        printf("spread key unpin page \n");
        return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
        if( return_code != RC_OK )
            printf("spreak keys failed error code %d", return_code);
        if( purge_check == false)
            purge_check = true;
        
        (&tree_mgmt->pHandle) -> pageNum = pageNumber;
        return_code =  unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
        if( return_code != RC_OK )
            printf("spreak keys failed error code %d", return_code);
        
        if( purge_check )
            return_code = purgeKey(tree, mid_point, &mid_key);
        
        return return_code;
    }
    
    printf("Spread key : COMPLETED \n");
    return RC_OK;
}

/* Function to find nearest key */
static PageNumber searchClosestKey(BTreeHandle *tree, PageNumber pageNumber) {
    printf("searchClosestKey STARTED \n");
    RC return_code;
    
    NewNode *new_node1;
    NewNode *new_node2;
    Node *node1;
    PageNumber adjacent;
    BTreeMgmt *tree_mgmt = (BTreeMgmt*)tree->mgmtData;
    int counter, pointer;
    
    return_code = pinPage(&tree_mgmt-> bMgr, &tree_mgmt -> pHandle, pageNumber);
    if( return_code != RC_OK )
        printf("searchClosestKey pin page 1: %d", return_code);
    
    new_node2 = (NewNode*)tree_mgmt -> pHandle.data;
    pointer = new_node2 -> pageNumber;
    
    return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
    if( return_code != RC_OK )
        printf("searchClosestKey unpin page 1: %d", return_code);
    
    if(pointer<0) {
        printf("searchClosestKey COMPLETE. Pointer less than 0 \n");
        return 0;
    }
    
    return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, pointer);
    if( return_code != RC_OK )
        printf("searchClosestKey pin page 2: %d", return_code);
    
    new_node1=(NewNode*) tree_mgmt -> pHandle.data;
    node1 = &new_node1 -> node;
    
    for(counter=0; counter < new_node1 -> keyNumber; counter++)
        if(pageNumber == node1[counter].ptr)
            break;
    
    if(counter == new_node1 -> keyNumber && pageNumber == new_node1 -> pageNumber1)
        adjacent = (int) node1[new_node1->keyNumber-1].ptr;
    else if(counter == new_node1 -> keyNumber - 1 )
        adjacent = - new_node1 -> pageNumber1;
    else if(counter==0)
        adjacent = - (int)node1[counter++].ptr;
    else
        adjacent = (int) node1[counter-1].ptr;
    
    return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
    if( return_code != RC_OK )
        printf("searchClosestKey unpin page 2: %d", return_code);
    
    printf("searchClosestKey COMPLETED \n");
    return adjacent;
}

/* Function to purge key */
static RC purgeKey(BTreeHandle *tree, PageNumber page_number, Value *key) {
    RC return_code = 0;
    int neg_one = -1;
    BTreeMgmt *tree_mgmt = (BTreeMgmt*) tree->mgmtData;
    
    printf("delete key calling purge key\n");
    return_code = purgeKey(tree, page_number, key);
    if( return_code != RC_OK )
        printf("delete key failed error is %d \n", return_code);
    
    tree_mgmt = (BTreeMgmt*) tree->mgmtData;
    printf("deleteKey pin page \n");
    return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, page_number);
    if( return_code != RC_OK )
        printf("delete key failed exit code %d /n", return_code);
    
    NewNode *new_node1;
    new_node1 = (NewNode*)&tree_mgmt -> pHandle.data;
    PageNumber page_number2;
    page_number2 = new_node1 -> pageNumber;
    printf("deleteKey remove node \n");
    int temp3 = deleteNode(tree, new_node1, page_number, key);
    int temp2 = new_node1 -> keyNumber;
    
    printf("deleteKey unpin page \n");
    return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
    if( return_code != RC_OK )
        printf("delete key failed exit code %d /n", return_code);
    
    if ( page_number2 <= 0 ){
        printf("delete key issue with delete key function as page_number shoule be > 0 \n");
    } else {
        printf("deleteKey pin page as page_number2 > 0\n");
        return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, page_number2);
        if( return_code != RC_OK )
            printf("delete key failed exit code %d /n", return_code);
        
        NewNode *new_node3;
        new_node3=(NewNode*)tree_mgmt -> pHandle.data;
        
        if(new_node1->keyNumber == 0 && new_node3->keyNumber==1) {
            printf("deleteKey node1: keyNumber =0 & node3: keyNumber =1\n");
            if(page_number != new_node3 -> node.ptr)
                tree_mgmt -> pageNumber = (int) new_node3 -> node.ptr;
            else
                tree_mgmt -> pageNumber = new_node3 -> pageNumber1;
            
            new_node1 -> pageNumber = neg_one;
            int temp = tree_mgmt -> nodeCount;
            temp = temp - 1;
            tree_mgmt->nodeCount = temp;
            temp = new_node3->keyNumber;
            temp = temp -1;
            new_node3 -> keyNumber = temp;
            
            if(new_node1 -> isLeaf)
                new_node1 -> pageNumber1 = neg_one;
        } else if( new_node1->keyNumber && temp3 == 0 ){
            printf("deleteKey node1: keyNumber  & temp3 = 0\n");
            Value equal;
            Node *node1;
            node1 = &new_node3 -> node;
            temp3 = 0;
            do{
                valueEquals(&node1[temp3].key, key, &equal);
                if(equal.v.boolV) {
                    node1[temp3].key = new_node1->node.key;
                    break;
                }
                temp3 = temp3+1;
            } while( temp3 < new_node3 -> keyNumber);
        }
        
        printf("deleteKey unpin page\n");
        return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
        if( return_code != RC_OK )
            printf("deleteKey finsided with error %d\n", return_code);
    }
    
    if(tree_mgmt -> pageNumber == page_number || new_node1 -> pageNumber < 0) {
        printf("deleteKey tree_mgmt pageNumber == pageNumber \n");
        if(tree_mgmt -> entryCount == 0) {
            int temp = 1;
            tree_mgmt -> pageNumber = temp;
            temp = 0;
            tree_mgmt->entryCount = temp;
            temp =0;
            tree_mgmt->nodeCount = temp;
        }
        
        if(new_node1 -> keyNumber == 1 && new_node1 -> pageNumber < 0) {
            printf("deleteKey pin page as keyNumber==1 && pageNumber < 0\n");
            int temp = (int) new_node1->node.ptr;
            return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, temp);
            if( return_code != RC_OK)
                printf("delete key failed error code %d\n", return_code);
            
            NewNode *new_node4;
            temp = new_node4 -> keyNumber;
            if( temp == 0) {
                tree_mgmt -> pageNumber = new_node1-> pageNumber1;
                new_node4 -> pageNumber = neg_one;
                temp = tree_mgmt -> nodeCount;
                temp = temp - 1;
                tree_mgmt -> nodeCount = temp;
                bool bol_val = new_node4 -> isLeaf;
                if(bol_val)
                    new_node4-> pageNumber1 = neg_one;
            }
            printf("deleteKey un pining page \n");
            return_code = unpinPage(tree, new_node1 -> node.ptr);
            if(return_code != RC_OK )
                printf("deleteKey failed %d\n", return_code);
            
            printf("deleteKey pin the page\n");
            temp = (int) new_node1 -> pageNumber1;
            return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, temp);
            if( return_code != RC_OK )
                printf("deleteKey failed %d\n", return_code);
            
            new_node4 = (NewNode*)tree_mgmt -> pHandle.data;
            temp = new_node4 -> keyNumber;
            if(temp == 0) {
                tree_mgmt-> pageNumber = (int) new_node1->node.ptr;
                new_node4 -> pageNumber = neg_one;
                temp = tree_mgmt -> nodeCount;
                temp = temp - 1;
                tree_mgmt->nodeCount = temp;
                bool bol_val = new_node4->isLeaf;
                if(bol_val)
                    new_node4-> pageNumber1 = neg_one;
            }
            
            printf("deleteKey unpin the page\n");
            return_code = unpinPage(&tree_mgmt->bMgr ,&tree_mgmt->pHandle);
            if( return_code != RC_OK )
                printf("deleteKey failed %d\n", return_code);
            
            printf("deleteKey pin the page pageNumber1\n");
            temp = new_node1-> pageNumber;
            return_code = pinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle, temp);
            if( return_code != RC_OK )
                printf("deleteKey failed %d\n", return_code);
            
            new_node4 = (NewNode*)tree_mgmt -> pHandle.data;
            temp = new_node4->keyNumber;
            if( temp == 0) {
                temp = (int) new_node1 -> node.ptr;
                tree_mgmt -> pageNumber = temp;
                new_node4 -> pageNumber = neg_one;
                temp = tree_mgmt -> nodeCount;
                temp = temp -1;
                tree_mgmt -> nodeCount = temp;
                bool bol_val = new_node4 -> isLeaf;
                if(bol_val)
                    new_node4 -> pageNumber1 = neg_one;
            }
            
            printf("deleteKey unpin the page\n");
            return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
            if( return_code != RC_OK )
                printf("deleteKey failed %d\n", return_code);
            
        }
        return(RC_OK);
    }
    
    int temp = 0;
    temp = tree_mgmt -> bTreeOrder;
    temp = temp +1;
    temp = temp /2;
    if( temp2 > temp )
        return RC_OK;
    
    PageNumber page_number1;
    printf("delete key find nearest key \n");
    page_number1 = searchClosestKey(tree, page_number);
    PageNumber page_neighbour;
    if(page_number1 >=0 )
        page_neighbour = page_number1;
    else
        page_neighbour = page_number1 * neg_one;
    
    printf("delete key pin page page_neighbour \n");
    return_code = pinPage(&tree_mgmt->bMgr,&tree_mgmt->pHandle, page_neighbour);
    if( return_code != RC_OK )
        printf("deleteKey failed %d\n", return_code);
    
    NewNode *new_node2 = (NewNode*)&tree_mgmt -> pHandle.data;
    int temp1 = new_node2 -> keyNumber;
    printf("delete key un pin the page\n");
    return_code = unpinPage(&tree_mgmt->bMgr, &tree_mgmt->pHandle);
    if( return_code != RC_OK )
        printf("deleteKey failed %d\n", return_code);
    
    int b_tree_order = tree_mgmt->bTreeOrder;
    int temp_val = temp1 + temp2;
    if ( temp_val <= b_tree_order) {
        printf("delete key join the keys \n");
        mergeKeys(tree, page_number1, page_number);
    } else if(temp2 < (b_tree_order+1/2)) {
        printf("delete key spreading the keys \n");
        distributeKeys(tree, page_number1, page_number);
    }
    
    return RC_OK;
}

