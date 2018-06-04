
#include "qe.h"
#include "string.h"
#include <algorithm>

bool Iterator::fieldIsNull(void *data, int i) {
	uint8_t nullByte = ((uint8_t *) data)[i / 8];
	uint8_t mask = 128 >> (i % 8);
	return (nullByte & mask) > 0;
}

void Iterator::setFieldNull(void *data, int i) {
	((char *) data)[i / 8] |= 128 >> (i % 8);
}

unsigned Iterator::getNumNullBytes(unsigned numAttributes) {
    return int(ceil((double) numAttributes / CHAR_BIT));
}

unsigned Iterator::getFieldLength(void *field, Attribute &attr) {
	unsigned length;
	switch (attr.type) {
	case TypeInt:
	case TypeReal:
		length = attr.length;
		break;
	case TypeVarChar: {
		uint32_t varcharLength;
		memcpy(&varcharLength, field, VARCHAR_LENGTH_SIZE);
		length = VARCHAR_LENGTH_SIZE + varcharLength;
		break;
	}
	}
	return length;
}

Filter::Filter(Iterator* input, const Condition &condition)
{
	
}

Filter::~Filter()
{
}

RC Filter::getNextTuple(void *data)
{
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{

}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->input = input;
	this->attrNames = attrNames;
	input->getAttributes(inputAttrs);
	inputTupleSize = 0;
	for (Attribute &attr: inputAttrs) {
		inputTupleSize += attr.length;
	}
}

Project::~Project()
{
}

RC Project::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);
	if (input->getNextTuple(origData) != QE_EOF) {
		if(projectAttributes(origData, data))
			return QE_EOF;
		return SUCCESS;
	}
	return QE_EOF;
}

RC Project::projectAttributes(void *origData, void *newData) {
	unsigned origNumNullBytes = getNumNullBytes(inputAttrs.size());
	unsigned newNumNullBytes = getNumNullBytes(attrNames.size());

	// set all null bytes in newData to 0
	memset(newData, 0, newNumNullBytes);

	// offset into newData
	// skip null bytes
	unsigned newOffset = newNumNullBytes;
	// iterate through list of new attribute names
	for (unsigned newIndex = 0; newIndex < attrNames.size(); newIndex++) {
		// offset into origData
		unsigned origOffset = origNumNullBytes;
		// iterate through original record descriptor
		unsigned origIndex;
		for (origIndex = 0; origIndex < inputAttrs.size(); origIndex++) {
			Attribute curAttr = inputAttrs[origIndex];
			bool nullField = fieldIsNull(origData, origIndex);
			// if this entry in the record descriptor matches the current attribute name
			// AND this field in the original tuple is null
			if (curAttr.name == attrNames[newIndex] && nullField) {
				// set this field to be null in the new tuple
				setFieldNull(newData, newIndex);
				// break from inner loop
				break;
			}
			// if this entry in the record descriptor matches the current attribute name
			// AND this field in the original tuple is NOT null
			else if (curAttr.name == attrNames[newIndex] && !nullField) {
				// copy this field from the old tuple to the new one
				unsigned fieldSize = getFieldLength((char *) origData + origOffset, curAttr);
				memcpy((char *) newData + newOffset, (char *) origData + origOffset, fieldSize);
				newOffset += fieldSize;
				// break from inner loop
				break;
			}
			// if neither of these conditions are met and this field isn't null, advance origOffset and loop
			if (!nullField) {
				unsigned fieldLength = getFieldLength((char *) origData + origOffset, curAttr);
				origOffset += fieldLength;
			}
		}
		if (origIndex == inputAttrs.size())
			return QE_ATTR_NOT_FOUND;
	}

	return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for (string attrName: attrNames) {
		auto pred = [&](Attribute attr) { return attr.name == attrName; };
		vector<Attribute>::const_iterator attr = find_if(inputAttrs.begin(), inputAttrs.end(), pred);
		attrs.push_back(*attr);
	}
}

INLJoin::INLJoin(Iterator *leftIn,
	  IndexScan *rightIn,
	  const Condition &condition)
{
	// Verify that the condition contains two attributes (it is a JOIN).
	if(!condition.bRhsIsAttr);

	// Check if right attribute matches condition.
	if(rightIn->attrName == condition.rhsAttr);

	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	this->cond = condition;
	inputTupleSize = 0;

	for(Attribute attr: leftAttrs) {
		inputTupleSize += attr.length;
	}

	// Check if all attributes are the same.
	for(int i = 0; i < leftAttrs.size() || i < rightIn->attrs.size(); i++){
		if(leftAttrs[i].name == rightIn->attrs[i].name)
			continue;
	}
}

INLJoin::~INLJoin()
{
}

RC INLJoin::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);
	IndexScan *riCopy = rightIn;

	if(leftIn->getNextTuple(origData) != QE_EOF) {

		// If we are not one checking every tuple in the rightIn,
		// we continue scanning through that.
		if(rightIn->getNextTuple(origData) != QE_EOF)

			if(join(origData, data))
				return QE_EOF;

		// Otherwise, we reset rightIn for the next tuple to be
		// scanned.
		riCopy = new IndexScan(rightIn->rm, rightIn->tableName,
									rightIn->attrName);
	}
	return SUCCESS;
}

RC INLJoin::join(void *origData, void *newData){

	return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	// Setting attrs to all attributes represented in inner and outer tables.
	attrs.clear();
	attrs = this->leftAttrs;
	attrs.insert(attrs.end(), (rightIn->attrs).begin(), (rightIn->attrs).end());

}