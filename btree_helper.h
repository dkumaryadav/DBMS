//
//  btree_helper.h
//  RecordManager
//
//  Created by Deepak Kumar Yadav on 11/18/19.
//  Copyright © 2019 DeepakKumarYadav. All rights reserved.
//

#ifndef btree_helper_h
#define btree_helper_h

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

// Strucutre for binary tree information
typedef struct BTreeMgmt {
    BM_BufferPool bMgr;
    BM_PageHandle pHandle;
    PageNumber pageNumber;
    int nodeCount;
    int entryCount;
    int bTreeOrder;
}BTreeMgmt;

typedef struct Node {
    Value key;
    long long ptr;
}Node;

typedef struct BTreeScanMgmt {
    Node *currentNode;
    PageNumber pageNumber;
    int currentPosition;
}BTreeScanMgmt;

typedef struct NewNode {
    Node node;
    PageNumber pageNumber;
    PageNumber pageNumber1;
    int keyNumber;
    bool isLeaf;
}NewNode;

static int fillMemory(void *pointer, int value, int size);
static int moveMemory(void *destination, void * source, int size);
static RC locateKey (BTreeHandle *tree, NewNode *temp_nn1, Value *key, int *epislon_value, int *temp_pn2, bool matched);
static int makeRoot(BTreeHandle *tree, NewNode *newNode1, long long ptr, Value *key );
static RC updateRoot(BTreeHandle *tree, PageNumber pageN, PageNumber pageNumber, Value key);
static RC splitThenMerge(BTreeHandle *tree, PageNumber pageN, bool isLeaf);
static RC deleteNode(BTreeHandle *tree, NewNode *new_node1, PageNumber pageNumber, Value *key );
static RC mergeKeys(BTreeHandle *tree, PageNumber lp, PageNumber pageNumber);
static int memoryCopy( void * pointer1, void *pointer2, int size);
static RC distributeKeys(BTreeHandle *tree, PageNumber lp, PageNumber pageNumber);
static PageNumber searchClosestKey(BTreeHandle *tree, PageNumber pageNumber);
static RC purgeKey(BTreeHandle *tree, PageNumber fp, Value *key);
static int addBTNode(BTreeHandle *tree);

#endif /* btree_helper_h */

