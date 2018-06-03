#include <stdio.h>
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
    // Check for file existence
    if (fopen(fileName.c_str(), "r") != nullptr) {
        eprintf("createFile: file %s already exists\n", fileName.c_str());
        return -1;
    }

    FILE* newFile = fopen(fileName.c_str(), "w");
    if (newFile == nullptr) {
        eprintf("createFile: %s creation failed", fileName.c_str());
        return -2;
    }
    fclose(newFile);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(remove(fileName.c_str()) != 0){
        eprintf("destroyFile: %s destruction failed", fileName.c_str());
        return -1;
    }
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(fileHandle.fp != NULL){
        eprintf("openFile: FileHandle already has a file");
        return -1;
    }

    if((fileHandle.fp = fopen(fileName.c_str(), "rwa+")) == nullptr){
        eprintf("openFile: could not open %s", fileName.c_str());
        return -2;
    } 

    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.fp == NULL) {
        eprintf("closeFile: fileHandle has not opened a file");
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
    // If page doesn't exist, return error.
    if(pageNum < 0 || pageNum > getNumberOfPages()){
        eprintf("readpage: page %d is not a page", pageNum);
        return -1;
    }
    if(fseek(this->fp, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        eprintf("readPage: seek failed");
        return -2;
    }
    if(fread(data, 1, PAGE_SIZE, this->fp) != PAGE_SIZE) {
        eprintf("readPage: read failed");
        return -3;
    }

    this->readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(fseek(this->fp, pageNum * PAGE_SIZE, SEEK_SET) != 0){
        eprintf("writePage: seek failed");
        return -1;
    }
    if(fwrite(data, 1, PAGE_SIZE, this->fp) != PAGE_SIZE){
        eprintf("writePage: write failed");
        return -2;
    }
    if(fflush(this->fp) != 0) {
        eprintf("writePage: flush failed");
        return -3;
    }

    this->writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if (this->fp == NULL) {
        eprintf("appendPage: no file attached to FileHandle");
    }
    if (fseek(this->fp, 0L, SEEK_END) != 0) {
        perror("appendPage: seek failed");
        return -1;
    }
    if (fwrite(data, 1, PAGE_SIZE, this->fp) != PAGE_SIZE) {
        perror("appendPage: write failed");
        return -2;
    }
    if(fflush(this->fp) != 0) {
        perror("appendPage: flush failed");
        return -3;
    }
    this->appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    unsigned fileSize;

    fseek(this->fp, 0L, SEEK_END);
    fileSize = ftell(this->fp);
    if(fileSize < 0){
        eprintf("getNumberOfPages: could not read number of pages");
        return -1;
    }
    return fileSize / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
