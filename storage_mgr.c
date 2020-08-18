#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "dberror.h"
#include "storage_mgr.h"

int init=0;

/*
 * Function checkinit()
 * Check the value of Global Variable used for initialization of storage manager.
*/
int checkinit (void)
{
    if (init == 1)
    	return RC_OK;
    else
    	return -1;
}

/*
 * Function exists()
 * Check if the given file exists or not.
*/
int exists(const char *fname)
{
	if( access( fname, F_OK ) != -1 ) {
		return 1;
	} else {
		return 0;
	}
}

/*
 * Function delete()
 * Deletes the specified file.
*/
int delete(const char *fname)
{
	int rrem=-1;
	rrem=remove(fname);
	return rrem;
}

/************************************************************
 *                   Interface								*
 ************************************************************/

/*
 * Function initStorageManager()
 * The function will check if the storage manager is initialized.
*/
void initStorageManager (void)
{
    if (checkinit() != RC_OK)
    	{
    		init=1;
    		printf("Storage manager is initialized\n");
    	}
    else
    	printf("Storage manager is already initialized\n");
}

/*
 * Function createPageFile()
 * The function will Create a file with empty block of 4096 Bytes.
*/
RC createPageFile (char *fileName)
{
	int i;
	char emptyblock[PAGE_SIZE];
	int writef;
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			return RC_FILE_PRESENT;
		}
		else
		{
			FILE *fp=fopen(fileName, "wb");
			for(i=0;i<PAGE_SIZE;i++)
			{
				emptyblock[i]=0;
			}
			writef=fwrite(emptyblock, 1, PAGE_SIZE, fp);
			        if (writef < PAGE_SIZE)
			        {
			          fclose(fp);
			          return RC_WRITE_FAILED;
			        }
			return RC_OK;
		}
	}
	else
	{
		return RC_STORAGE_MGR_NOT_INIT;
	}
}

/*
 * Function openPageFile()
 * Opens the Created file in read/write mode and assigns the file details in the File Handler required for further file processing
*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	int readl = 0;
	if (checkinit() == RC_OK)
		{
			if (exists(fileName))
			{
				FILE *fp=fopen(fileName, "rb+");

				readl=fseek(fp,0,SEEK_END);

				//fseek(fp, 0, SEEK_END);
				fHandle->totalNumPages = (int) (ftell(fp)/PAGE_SIZE);
				fseek(fp, 0, SEEK_SET);
				fHandle->curPagePos = 0;
				fHandle->fileName = fileName;
				fHandle->mgmtInfo = fp;
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

/*
 * Function closePageFile()
 * Closes the File, resets the file handler and cleans the system memory.
*/
RC closePageFile (SM_FileHandle *fHandle)
{
	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				fclose(fHandle->mgmtInfo);
				fHandle->totalNumPages=-1;
				fHandle->curPagePos=-1;
				fHandle->curPagePos='\0';
				fHandle->mgmtInfo=NULL;
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

/*
 * Function destroyPageFile()
 * Deletes the file from the system.
*/
RC destroyPageFile (char *fileName)
{
	int rd;
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			rd=delete(fileName);
			if(rd==0)
			return RC_OK;
			else
			return RC_DELETE_FAILED;
		}
		else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		return RC_STORAGE_MGR_NOT_INIT;
	}
	return RC_OK;
}

/*
 * Function readBlock()
 * Reads the content of specified file page in the Page handler.
*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	char emptyblock[PAGE_SIZE];
	FILE *fd=fHandle->mgmtInfo;
	int seekpage= PAGE_SIZE*pageNum;
	int readp;
	int readl;
	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				if((pageNum<fHandle->totalNumPages) && (pageNum>=0))
				{
					readl=fseek(fd,seekpage,SEEK_SET);
					readp=fread(memPage,1,PAGE_SIZE, fd);
					if (readp < PAGE_SIZE)
				        return RC_READ_FAILED;
					fHandle->curPagePos=pageNum;
					return RC_OK;
				}
				else
				{
					return RC_READ_NON_EXISTING_PAGE;
				}
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

/*
 * Function getBlockPos()
 * Get the current Page number of the file where the file handler points.
*/
int getBlockPos (SM_FileHandle *fHandle)
{
	if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					return(fHandle->curPagePos);
				}
				else
				{
					return RC_FILE_NOT_FOUND;
				}
			}
	else
	{
				return RC_STORAGE_MGR_NOT_INIT;
	}
}

/*
 * Function readFirstBlock()
 * Reads the first page into the Page handler.
*/
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (0, fHandle, memPage));
}

/*
 * Function readPreviousBlock()
 * Reads the previous page if exists into the Page handler.
*/
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock ((fHandle->curPagePos-1), fHandle, memPage));
}

/*
 * Function readCurrentBlock()
 * Reads the content of the current page from the file handler into the Page handler.
*/
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (fHandle->curPagePos, fHandle, memPage));
}

/*
 * Function readNextBlock()
 * Reads the contents of the next page from the file handler into the Page handler.
*/
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock ((fHandle->curPagePos+1), fHandle, memPage));
}

/*
 * Function readLastBlock()
 * Reads the last page of the file from the file handler into the Page handler.
*/
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (fHandle->totalNumPages-1, fHandle, memPage));
}

/*
 * Function writeBlock()
 * Writes the data from the Page Handler to the file.
*/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *fd=fHandle->mgmtInfo;
	int seekpage= PAGE_SIZE*pageNum;
	int readp;
	int readl;

	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				if(pageNum>=0)
				{
					readl=fseek(fd,seekpage,SEEK_SET);
					if(pageNum>=fHandle->totalNumPages) //to create pages // checking
					{
						fHandle->totalNumPages=pageNum+1;
					}
					readp=fwrite(memPage,1,PAGE_SIZE, fd);
				    if ( readp< PAGE_SIZE)
				        return RC_WRITE_FAILED;
					fHandle->curPagePos=pageNum;
					return RC_OK;
				}
				else
				{
					return RC_INVALID_PAGE_NUMBER;
				}
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

/*
 * Function writeCurrentBlock()
 * Writes the data present of the specified page from the Page Handler to the file.
*/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(writeBlock (fHandle->curPagePos, fHandle, memPage));
}

/*
 * Function appendEmptyBlock()
 * Appends an empty page of specified Page Size into the File.
*/
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	int i;
	char emptyblock[PAGE_SIZE];
	for(i=0;i<PAGE_SIZE;i++)
		emptyblock[i]=0;

	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				writeBlock (fHandle->totalNumPages, fHandle, emptyblock);
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

/*
 * Function ensureCapacity()
 * Checks if the number of the pages in the file is equal to the specified Page Number
 * Appends new blocks to the file if it is less than specified capacity.
*/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	int i;
		char emptyblock[PAGE_SIZE];
		for(i=0;i<PAGE_SIZE;i++)
			emptyblock[i]=0;

		if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					writeBlock (numberOfPages-1, fHandle, emptyblock);
					return RC_OK;
				}
				else
				{
					return RC_FILE_NOT_FOUND;
				}
			}
			else
			{
				return RC_STORAGE_MGR_NOT_INIT;
			}
}
