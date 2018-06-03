
#include "qe.h"
#include "string.h"
#include <algorithm>

bool Iterator::fieldIsNull(void *data, int i) {
	return ((char *) data)[i / 8] & (128 >> (i % 8)) > 0;
}

void Iterator::setFieldNull(void *data, int i) {
	((char *) data)[i / 8] |= 128 >> (i % 8);
}

unsigned Iterator::getNumNullBytes(unsigned numAttributes) {
    return int(ceil((double) numAttributes / CHAR_BIT));
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
	for (Attribute attr: inputAttrs) {
		inputTupleSize += attr.length;
	}
}

Project::~Project()
{
}

RC Project::getNextTuple(void *data)
{
	void *origData = malloc(inputTupleSize);
	while (input->getNextTuple(origData) != QE_EOF) {
		if(projectAttributes(origData, data))
			return QE_EOF;
	}
	return QE_EOF;
}

RC Project::projectAttributes(void *origData, void *data) {
	unsigned origNumNullBytes = getNumNullBytes(inputAttrs.size());
	unsigned newNumNullBytes = getNumNullBytes(attrNames.size());

	// set all null bytes in data to 0
	memset(data, 0, newNumNullBytes);
	for (unsigned newIndex = 0; newIndex < attrNames.size(); newIndex++) {
		// offset into origData
		// skip null bytes
		unsigned offset = origNumNullBytes;
		for (unsigned oldIndex = 0; oldIndex < inputAttrs.size(); oldIndex++) {
			if (inputAttrs[oldIndex].name == attrNames[newIndex]) {

			}

		}
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
//	if(!(*condition)->bRhsIsAttr);

	// Check if right attribute matches condition.
//	if(!strcmp(rightIn->attrName, (*condition)->rhsAttr));

	// Check that attributes are comparable.
//	if(condition->rhsValue->type != condition->lhsValue->type);

	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(leftAttrs);
	this->cond = cond;
	inputTupleSize = 0;
	for(Attribute attr: leftAttrs) {
		inputTupleSize += attr.length;
	}

	// Check if all attributes are the same.
	for(int i = 0; i < leftAttrs.size() || i < rightIn->attrs.size(); i++){

		if(strcmp(leftAttrs[i], rightIn->attrs[i]))
			continue;
	}
}

INLJoin::~INLJoin()
{
}

RC INLJoin::getNextTuple(void *data)
{
	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
}