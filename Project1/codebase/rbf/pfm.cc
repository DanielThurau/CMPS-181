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
        cerr << "createFile: file " << fileName << " already exists" << endl;
        return -1;
    }
    FILE* newFile = fopen(fileName.c_str(), "w");
    if (newFile == NULL) {
        perror("createFile: file creation failed");
        return -2;
    }
    fclose(newFile);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(remove(fileName.c_str()) != 0){
        perror("destroyFile: file destruction failed");
        return -1;
    }
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(fileHandle.fp == NULL){
        perror("fileHandle already handle for file");
        return -1;
    }

    if((fileHandle.fp = fopen(fileName.c_str(), "rwa+")) == nullptr){
        perror("fileHandle: could not open file");
        return -1;
    } 

    return 1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.fp == NULL) {
        cerr << "closeFile: fileHandle has not opened a file";
        return -1;
    }
    fclose(fileHandle.fp);
    fileHandle.fp = NULL;
    return 0;
}


FileHandle::FileHandle()
{
    fp = NULL;
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(fseek(this->fp, pageNum*PAGE_SIZE, SEEK_SET) != 0){
        perror("FileHandle::writePage page: does not exist");
        return -1;
    }

    if(fwrite(data, PAGE_SIZE, PAGE_SIZE, this->fp) != PAGE_SIZE){
        perror("FileHandle::writePage page: could not write page");
        return -1;
    }

    this->writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}
