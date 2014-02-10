/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "operators.h"
#include "operators_priv.h"

#include <sstream>
#include <iostream>
using namespace std;

//#define VERBOSE

unsigned short parseSortInput(const string& s)
{
	size_t l = s.find('$');
	if (l == string::npos)
		return -1;
	string remainder = s.substr(l+1);

	unsigned short ret = -1;
	istringstream ss(remainder);
	ss >> ret;
	return ss ? ret : -1;
}

void SortLimit::init(libconfig::Config& root, libconfig::Setting& cfg)
{

	Operator::init(root, cfg);

	schema = nextOp->getOutSchema();

    //get the sort order list
    libconfig::Setting& field = cfg["by"];
    for (int i=0; i<field.getLength(); i+=1)
    {
		string projattrstr = field[i];
        orderby.push_back(parseSortInput(projattrstr));
    }

    //get whether or not its ascending
    asc = (1==(int)cfg["asc"][0]);

    //get the limit
    limit = cfg["limit"][0];

    for (int i=0; i<MAX_THREADS; ++i) {
        output.push_back(NULL);
        state.push_back(State(0, Ready, 0));
	}

    sortnodesize = 0;

    for (int i = 0 ; i < orderby.size() ; i += 1){
        allpossible.push_back( vector<Comparator::Comparison>());
    }
    int equalpos = 0;
    while (equalpos < orderby.size() ){
        vector<Comparator::Comparison> &op = allpossible[equalpos];
        int i;
        for (i = 0  ; i < orderby.size()  ; i += 1){
            if (i < equalpos){
                op.push_back(Comparator::Equal);
            }
            else{
                if (asc)
                    op.push_back(Comparator::Less);
                else
                    op.push_back(Comparator::Greater);
                break;
            }
        }
        equalpos += 1;
    }
    for (int i = 0 ; i < orderby.size() ; i += 1){
        faster.push_back(ConjunctionEvaluator());
        faster.back().init(schema, schema, orderby, orderby, allpossible[i]);
    }
}

SortLimit::sortnode* SortLimit::createNode(void* tuple, int size){
    sortnode* t = new sortnode();
    t->payload = reinterpret_cast<void*>(new char[schema.getTupleSize()]);
    memcpy(t->payload, tuple, schema.getTupleSize());
    return t;
};

void SortLimit::sortHelper(void* tuple)
{
    if (sortnodesize == limit){
        //might not even need to create another sortnode
        //run search first before materialize and copy

        sortnode* c = head;
        bool aheadof ;
        while (true){
            aheadof = false;
            for (int i = 0 ; i < orderby.size()  ; i += 1){
                //evaluator.init(schema, schema, orderby, orderby,allpossible[i]); 
                if (faster[i].eval(tuple,c->payload)){
                    aheadof = true;
                    break;
                }
            }
            if (aheadof) break;
            if (c->next!= NULL){
                c = c->next;
            }else{
                break;
            }
        }

        //decided we need to add 
        if (aheadof){
            sortnode* t =    createNode(tuple, schema.getTupleSize());
            if (c == head){
                t->next = head;
                head->prev = t;
                head = t;
            }
            else {
                c->prev->next = t;
                t->prev = c->prev;
                t->next = c;
                c->prev = t;
            }

            last = last->prev;
            last->next= NULL;
        }

    }
    //all is empty, there is no head
    else if (sortnodesize == 0){
        head = createNode(tuple, schema.getTupleSize());
        last = head;
        sortnodesize += 1;
    }
    //not yet hit the limit
    else if (sortnodesize < limit){
        sortnode* t = createNode(tuple, schema.getTupleSize());

        //need to sort now
        //for each node already collected
        sortnode* c = head;
        bool aheadof ;
        while (true){
            aheadof = false;
            for (int i = 0 ; i < orderby.size()  ; i += 1){
                //evaluator.init(schema, schema, orderby, orderby,allpossible[i]); 
                if (faster[i].eval(t->payload,c->payload)){
                    aheadof = true;
                    break;
                }
            }
            if (aheadof) break;
            if (c->next!= NULL){
                c = c->next;
            }else{
                break;
            }
        }

        if (aheadof){
            if (c == head){
                t->next = head;
                head->prev = t;
                head = t;
            }
            else {
                c->prev->next = t;
                t->prev = c->prev;
                t->next = c;
                c->prev = t;
            }
        }
        else{
            c->next = t;
            t->prev = c;
            last = t;
        }

        sortnodesize += 1;
    }

    else{
        //WTF?!?!?
        cerr<<"sort op, shouldn't have gotten here\n";
        throw QueryExecutionError();
    }
};

Operator::ResultCode SortLimit::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
    // AT THE MOMENT ASSUME SORT LIMIT IS NOT PARALLEL CAPABLE, THREADID MUST = 0
    // IF WANT PARALLEL, MUST MAKE SORT LL HEAD AN ARRAY OF HEADS
    if (threadid > 0) {
        cerr<<"sort op, not parallel capable yet, read comment\n";
        throw NotYetImplemented();
    }

    ResultCode rescode;
	GetNextResultT result;
    void* tuple;
    Page* in;
    Operator::ResultCode rc;

    


    rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
    if (rescode == Operator::Error) {
        return Error;
    }

    result = nextOp->getNext(threadid);
    //consume all input and find the limit tuples
	while (result.first != Operator::Error) {
        in = result.second;
        unsigned int tupoffset=0;
        while ((tuple=in->getTupleOffset(tupoffset++))){
#ifdef VERBOSE
            cout << schema.prettyprint(tuple, ' ') << endl;
#endif
            
            sortHelper(tuple);

#ifdef VERBOSE
            sortnode* pdebug = head;
            for (int j = 0 ; j < sortnodesize ; j += 1){
                cout<<"\t\t"<<schema.prettyprint(pdebug->payload, ' ')<<endl;
                pdebug=pdebug->next;
            }
#endif
        }

        if (result.first == Operator::Finished) break;
        result = nextOp->getNext(threadid);
	}

	return rescode;

}


 Operator::GetNextResultT SortLimit::getNext(unsigned short threadid)
{

    Page* out = output[threadid];
    State& st = state[threadid];
    sortnode* t;

    out->clear();

    while (out->canStoreTuple() && sortnodesize > 0){
        memcpy(out->atomicAllocateTuple(), head->payload, schema.getTupleSize());
        t = head->next;
        if (t != NULL){
            t->prev = NULL;
        }
        delete head;
        head = t;
        sortnodesize --;
    }
    if (sortnodesize == 0)
        return make_pair(Finished, output[threadid]);
    else
        return make_pair(Ready, output[threadid]);
}

void SortLimit::threadInit(unsigned short threadid)
{
	output[threadid] = new Page(buffsize, schema.getTupleSize(), this);
}


void SortLimit::threadClose(unsigned short threadid)
{
    delete output[threadid];
}
