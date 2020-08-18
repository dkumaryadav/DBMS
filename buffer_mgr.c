#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
//#include <unistd.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
//#include <pthread.h>

int node_count=1;
int k=3;

/*typedef struct node
{
	BM_PageHandle *pg;
	int fixcount;
	bool is_dirty;
	int storage_pg_number;
	struct node *next;
	struct node *prev;
}node_dll;

typedef struct mgmtinfo {
	SM_FileHandle *fh;
	int read_io;
	int write_io;
	node_dll *head;
	//pthread_mutex_t bm_mutex;
}BM_mgmtinfo;*/

//#define BM_LOCK()   pthread_mutex_lock((pthread_mutex_t *)&((BM_mgmtinfo *)bm->mgmtData)->bm_mutex);
//#define BM_UNLOCK() pthread_mutex_unlock((pthread_mutex_t *)&((BM_mgmtinfo *)bm->mgmtData)->bm_mutex);

node_dll *GetNewNode(int x) {
	node_dll *newNode=(node_dll*)malloc(sizeof(node_dll));
	newNode->storage_pg_number = x;
	newNode->is_dirty=0;
	newNode->fixcount=0;
	newNode->prev = NULL;
	newNode->next = NULL;
	newNode->pg = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
	newNode->pg->pageNum=x;
	newNode->pg->data=0;
	return newNode;
 }

//Inserts a Node at tail of Doubly linked list
node_dll *InsertAtTail(node_dll *head, int pageNum) {
	node_dll *temp = head;
	node_dll *newNode = GetNewNode(pageNum);
	if(head == NULL) {
		head = newNode;
		return head;
	}
	while(temp->next != NULL)
		temp = temp->next; // Go To last Node
	temp->next = newNode;
	newNode->prev = temp;
	node_count +=1;
	return head;
}

RC strategy(BM_BufferPool *const bm, int pageNum) // k is only for LRU-K
{
	int if_fix=0;
	int k_iterations=1;
	node_dll *temp=((BM_mgmtinfo *)bm->mgmtData)->head;
	node_dll *temp1=((BM_mgmtinfo *)bm->mgmtData)->head;
	if(bm->strategy==RS_FIFO || bm->strategy==RS_LRU)
	{
		while(temp->fixcount!=0)
		{
			temp=temp->next;
			if_fix=1;
		}
				node_dll *temp_first=NULL;
				node_dll *temp_trav=NULL;
				node_dll *temp_prev=NULL;
				node_dll *temp_next=NULL;
				if(temp->prev != NULL)
					temp_prev=temp->prev;
				else
					temp_prev=((BM_mgmtinfo *)bm->mgmtData)->head;

				if(temp->next != NULL)
					temp_next=temp->next;

				temp_trav=temp;

				if(if_fix==1)
				{
					temp_first=temp_trav;
					if (temp_next != NULL){
						temp_prev->next = temp_next;
						temp_next->prev = temp_prev;
						temp_first->prev = NULL;
						temp_first->next = 	((BM_mgmtinfo *)bm->mgmtData)->head ;
						((BM_mgmtinfo *)bm->mgmtData)->head = temp_first;
					}
					else{
						temp_prev->next = NULL;
						temp_first->prev = NULL;
						temp_first->next = 	((BM_mgmtinfo *)bm->mgmtData)->head ;
						((BM_mgmtinfo *)bm->mgmtData)->head = temp_first;
					}
				}
		temp=((BM_mgmtinfo *)bm->mgmtData)->head;
		if(temp->is_dirty==1)
		{
			forcePage(bm,temp->pg);
		}

		((BM_mgmtinfo *)bm->mgmtData)->head=temp->next;
		((BM_mgmtinfo *)bm->mgmtData)->head->prev=NULL;
		temp->next=NULL;
		((BM_mgmtinfo *)bm->mgmtData)->head=InsertAtTail(((BM_mgmtinfo *)bm->mgmtData)->head, pageNum);
	}
	else if(bm->strategy==RS_LRU_K)
	{
		temp=((BM_mgmtinfo *)bm->mgmtData)->head;
		while(k_iterations < k )
		{
			k_iterations += 1;
			temp=temp->next;
		}
		while(temp->fixcount!=0)
		{
			temp=temp->prev;
			if_fix=1;
		}

		node_dll *temp_first;
		node_dll *temp_trav;
		node_dll *temp_prev;
		node_dll *temp_next;

		if(temp->prev != NULL)
			temp_prev=temp->prev;
		else
			temp_prev=((BM_mgmtinfo *)bm->mgmtData)->head;

		if(temp->next != NULL)
			temp_next=temp->next;

		temp_trav=temp;

		temp_first=temp_trav;
		temp_prev->next=temp_next;
		temp_next->prev=temp_prev;
		temp_first->prev=NULL;
		temp_first->next=((BM_mgmtinfo *)bm->mgmtData)->head;
		((BM_mgmtinfo *)bm->mgmtData)->head=temp_first;

		temp=((BM_mgmtinfo *)bm->mgmtData)->head;
		if(temp->is_dirty==1)
		{
			forcePage(bm,temp->pg);
		}
		((BM_mgmtinfo *)bm->mgmtData)->head=temp->next;
		((BM_mgmtinfo *)bm->mgmtData)->head->prev=NULL;
		temp->next=NULL;
		((BM_mgmtinfo *)bm->mgmtData)->head=InsertAtTail(((BM_mgmtinfo *)bm->mgmtData)->head, pageNum);

	}
return RC_OK;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	BM_mgmtinfo *mgmt;
	mgmt = (BM_mgmtinfo *)malloc(sizeof(BM_mgmtinfo));
	mgmt->fh = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));

	openPageFile(pageFileName, mgmt->fh);
	mgmt->read_io=0;
	mgmt->write_io=0;
	mgmt->head=NULL;
	bm->pageFile= pageFileName;
	bm->numPages= numPages;
	bm->strategy= strategy;
	bm->mgmtData = mgmt;
	node_count=1;
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	node_dll *temp;
	int page_found=0;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
	while(temp!=NULL)
	{
		temp->fixcount =0;
		temp=temp->next;
	}

	forceFlushPool(bm);
	((BM_mgmtinfo *)bm->mgmtData)->fh = NULL;
	bm->mgmtData = NULL;

	return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm)
{
	node_dll *temp;
	int page_found=0;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
	while(temp!=NULL)
	{
		if (temp->is_dirty==1)
		{
			if (temp->fixcount==0)
			{
				writeBlock (temp->storage_pg_number, ((BM_mgmtinfo *)bm->mgmtData)->fh,(SM_PageHandle) temp->pg->data);
				temp->is_dirty=0;
				((BM_mgmtinfo *)bm->mgmtData)->write_io += 1;
			}
		}
		temp=temp->next;
	}
		//BM_UNLOCK();
		return RC_OK;
	}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	node_dll *temp;
	int page_found=0;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
	while(temp!=NULL)
	{
		if (temp->storage_pg_number==page->pageNum)
		{
			temp->is_dirty=1;
			page_found=1;
			break;
		}
		temp=temp->next;
	}
	//BM_UNLOCK();
	if(page_found==1)
		return RC_OK;
	else
		return -1;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	node_dll *temp;
	int page_found=0;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
	while(temp!=NULL)
	{
		if (temp->storage_pg_number==page->pageNum)
		{
			temp->fixcount -= 1;
			page_found=1;
			break;
		}
		temp=temp->next;
	}
	//BM_UNLOCK();
	if(page_found==1)
		return RC_OK;
	else
		return -1;
}

//data from the buffer to file
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	node_dll *temp;
	int page_found=0;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;

	while(temp!=NULL)
	{
		if (temp->storage_pg_number==page->pageNum)
		{
			if(temp->is_dirty==1)
			{
				writeBlock (temp->storage_pg_number, ((BM_mgmtinfo *)bm->mgmtData)->fh,page->data);
				temp->is_dirty=0;
				page_found=1;
				break;
			}
		}
		temp=temp->next;
	}
	//BM_UNLOCK();
	if(page_found==1)
	{
		((BM_mgmtinfo *)bm->mgmtData)->write_io += 1;
		return RC_OK;
	}
	else
		return -1;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum)
{
char emptyblock[PAGE_SIZE];
int i;
//BM_LOCK();
node_dll *temp;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
	int PAGE_FOUND=0;
	if(pageNum >= ((BM_mgmtinfo *)bm->mgmtData)->fh->totalNumPages) //typecast void to the management info structure
	{
		ensureCapacity (pageNum+1, ((BM_mgmtinfo *)bm->mgmtData)->fh);
	}
	else
	{
		if(temp!=NULL)
		{
			while (temp!=NULL)
				{
					if(temp->storage_pg_number==pageNum)
					{
						PAGE_FOUND=1;
						break;
					}
					else
					{
						temp=temp->next;
					}
				}

		}
	}
	if(PAGE_FOUND==0)
	{
		if(node_count < bm->numPages)
		{
			((BM_mgmtinfo *)bm->mgmtData)->head = InsertAtTail(((BM_mgmtinfo *)bm->mgmtData)->head, pageNum);
			temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
			if(temp->next!=NULL)
			{
				while(temp->next!=NULL)
				{
					temp=temp->next;
				}
			}
		}
		else
		{
			strategy(bm, pageNum);
			temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
			while(temp->next!=NULL)
			{
				temp=temp->next;
			}
		}
	}
	temp->fixcount += 1;

	//Read file data into the Buffer
	for (i=0;i<PAGE_SIZE;i++)
		emptyblock[i]=0;

	temp->pg->data=emptyblock;
	readBlock (pageNum, ((BM_mgmtinfo *)bm->mgmtData)->fh,  temp->pg->data);
	page->data=temp->pg->data;
	page->pageNum = pageNum;

	if(PAGE_FOUND==0)
	{
		((BM_mgmtinfo *)bm->mgmtData)->read_io +=  1;
	}

	node_dll *temp_last;
	node_dll *temp_trav;
	node_dll *temp_prev;
	node_dll *temp_next;

	if(temp->prev != NULL)
		temp_prev=temp->prev;

	if(temp->next != NULL)
		temp_next=temp->next;

	temp_trav=temp;
	if(bm->strategy==RS_LRU ||bm->strategy==RS_LRU_K )
	{
		if(temp->next!=NULL)
		{
			while(temp_trav->next!=NULL)
			{
				temp_trav=temp_trav->next;
			}

			if(temp->prev != NULL)
			{
				temp_last=temp_trav;
				temp_prev->next=temp_next;
				temp_next->prev=temp_prev;
				temp->prev=temp_last;
				temp_last->next=temp;
				temp->next=NULL;
			}
			else
			{
				temp_next=temp->next;
				temp_last=temp_trav;
				temp->next=NULL;
				temp_next->prev=NULL;
				temp_last->next=temp;
				temp->prev=temp_last;
				((BM_mgmtinfo *)bm->mgmtData)->head = temp_next;
			}
		}
	}

	 //BM_UNLOCK();
	return RC_OK;
}

//returns the number of read IO done
int getNumReadIO (BM_BufferPool *const bm)
{
	if(bm->mgmtData != NULL)
		return ((BM_mgmtinfo *)bm->mgmtData)->read_io;
	else
		return 0;
}
// returns the number of writes IO done
int getNumWriteIO (BM_BufferPool *const bm)
{
	if(bm->mgmtData != NULL)
		return ((BM_mgmtinfo *)bm->mgmtData)->write_io;
	else
		return 0;
}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	node_dll *temp=((BM_mgmtinfo *)bm->mgmtData)->head;
	int i = bm->numPages;
	PageNumber *pn;//array that should be return

	if(((BM_mgmtinfo *)bm->mgmtData)->head == NULL)
		return (PageNumber)-1;
		//return NO_PAGE;

	pn = (PageNumber *)malloc(sizeof(PageNumber)*bm->numPages);
	while(i >= 0)
	{
		pn[i] = -1;
		i--;
	}

	i = 0;
	while (temp!= NULL)//going to each node
	{
		pn[i] = temp->storage_pg_number;//checking if page handle has a value
		i++;
		temp = temp->next;//next
	}

	return pn;
}

//returns whether the page is dirty or not
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	int i = 0, n;
	bool *dirt;//array that should be return
	node_dll *temp;
	temp = ((BM_mgmtinfo *)bm->mgmtData)->head;//Initialization to the start of the buffer
	n = bm->numPages;

	dirt = (bool *)malloc(sizeof(bool)*n);
	while(i < n)
	{
		dirt[i] = FALSE;
		i++;
	}
	i = 0;
	while (temp != NULL)//going to each node
	{
		if(temp->is_dirty)
			dirt[i] = TRUE;//storing the dirty values in the array
		i++;
		temp=temp->next;
	}

	return dirt;
}

//returns the number of present request on a particular page
int *getFixCounts (BM_BufferPool *const bm)
{
	node_dll *temp;
	int i = 0, n;
    int *fix;//array that should be return
    temp = ((BM_mgmtinfo *)bm->mgmtData)->head;
    n = bm->numPages;

    fix = (int *)malloc(sizeof(int)*n);
	//setting all fix as zero
    while(i < n)
    {
    	fix[i] = 0;
    	i++;
    }

    i = 0;
    while (temp!= NULL)//going to each node
    {
    	if(temp->fixcount > 0)
        	fix[i] = temp->fixcount;//storing the dirty values in the array
        i++;
        temp=temp->next;
    }
    return fix;
}
