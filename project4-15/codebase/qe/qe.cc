
#include "qe.h"
#include <string>

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
	return;
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
}

Project::~Project()
{
}

RC Project::getNextTuple(void *data)
{
	return QE_EOF;
}

void getAttributes(vector<Attribute> &attrs) const
{
	return;
}

INLJoin::INLJoin(Iterator *leftIn,
	  IndexScan *rightIn,
	  const Condition &condition)
{
	// Verify that the condition contains two attributes (it is a JOIN).
//	if(!(*condition).bRhsIsAttr);

	// Check if right attribute matches condition.
//	if(!rightIn.attrName.strcmp(*condition.rhsAttr));

	// Check that attributes are comparable.
//	if((*condition).rhsValue.type != (*condition).lhsValue.type);

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
	return;
}