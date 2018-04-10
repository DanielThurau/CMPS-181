#include <stdio.h>
#include <iostream>
#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if (fopen(fileName.c_str(), "w") != NULL) {
        cerr << "file " << fileName << " already exists";
        return -1;
    }
    FILE* newFile = fopen(fileName.c_str(), "w");
    if (newFile == NULL) {
        perror("file creation failed");
        return -2;
    }
    fclose(newFile);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{

//    int numPages = getNumberOfPages();

    // If page doesn't exist, return error.
//    if(pageNum < 0 || pageNum < numPages){
//        return -1;
//    }
    readPageCounter += 1;
    return 0;

    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
//    fseek();
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}
