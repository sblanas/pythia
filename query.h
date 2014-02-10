
/*
 * Copyright 2009, Pythia authors (see AUTHORS file).
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

#include "operators/operators.h"
#include "visitors/allvisitors.h"

#include <map>

class Query 
{
	public:
		Query() 
			: tree(0) 
		{ 
		}

		inline void threadInit()
		{
			ThreadInitVisitor tiv(0);
			accept(&tiv);
		}

		inline Operator::ResultCode scanStart()
		{
			Schema emptyschema;
			return tree->scanStart(0, NULL, emptyschema);
		}

		inline Operator::GetNextResultT getNext()
		{
			return tree->getNext(0);
		}

		inline Operator::ResultCode scanStop()
		{
			return tree->scanStop(0);
		}

		inline void threadClose()
		{
			ThreadCloseVisitor tcv(0);
			accept(&tcv);
		}

		/**
		 * Method that calls destroy() at each operator, but does not reclaim
		 * each operator object using `delete`. Useful for cheap copies of the
		 * tree and for statically allocated operators.
		 */
		inline void destroynofree()
		{
			RecursiveDestroyVisitor rdv;
			accept(&rdv);
		}

		inline void destroy()
		{
			RecursiveDestroyVisitor rdv;
			accept(&rdv);

			RecursiveFreeVisitor rfv;
			accept(&rfv);
		}

		inline Schema& getOutSchema() 
		{ 
			return tree->getOutSchema(); 
		}

		inline void accept(Visitor* v)
		{
			tree->accept(v);
		}

		typedef std::map<std::string, SingleInputOp*> UserDefinedOpMapT;
		void create(libconfig::Config& cfg, UserDefinedOpMapT& udops);

		/**
		 * Calls create with no user-defined operators.
		 */
		inline void create(libconfig::Config& cfg)
		{
			UserDefinedOpMapT emptyops;
			create(cfg, emptyops);
		}

		/**
		 * Returns operator depth in query tree, or -1 if it doesn't exist.
		 */
		int getOperatorDepth(Operator* op);

	// Leaving tree as public member until a helper method is developed
	// that can automatically construct and initialize a Query tree from the
	// configuration file.
	//
	// protected:
		Operator* tree;

		typedef std::map<Operator*, int> OperatorDepthT;
	private:
		// Map used for pretty-printing and debugging.
		//
		OperatorDepthT operatorDepth;
};
