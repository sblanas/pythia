
/*
 * Copyright 2012, Pythia authors (see AUTHORS file).
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

#include <pthread.h>
#include "custom_asserts.h"

template <typename T, int size>
class ParallelQueue
{
	public:
		ParallelQueue()
			: prodpointer(0), conspointer(0), rundown(false)
		{
			dbgassert(!pthread_mutex_init(&lock, NULL));
			dbgassert(!pthread_cond_init(&queueempty, NULL));
			dbgassert(!pthread_cond_init(&queuefull, NULL));
		}

		~ParallelQueue()
		{
			dbgassert(!pthread_mutex_destroy(&lock));
			dbgassert(!pthread_cond_destroy(&queueempty));
			dbgassert(!pthread_cond_destroy(&queuefull));
		}

		enum ResultT
		{
			Okay,
			Rundown
		};

		/**
		 * Pushes what is pointed to by \a datain in the queue. If queue is
		 * empty, call will block. If queue is in rundown, will return with
		 * Rundown.
		 */
		ResultT push(T* datain);

		/**
		 * Pops one item out of the queue, and writes it to the space pointed
		 * to by \a dataout. If queue is empty, then the behavior depends on
		 * whether the queue is is rundown. If not in rundown, the call will
		 * block until a new item is inserted in the queue. If the queue is in
		 * rundown, it will return immediately with Rundown.
		 */
		ResultT pop(T* dataout);

		/**
		 * Sets queue in rundown mode, and signals all waiters that the queue
		 * is in rundown. All pending and future push/pop operations will
		 * return Rundown and fail.
		 */
		void signalRundown();

	private:
		pthread_mutex_t lock;
		pthread_cond_t queueempty;
		pthread_cond_t queuefull;

		T queue[size];

		int prodpointer; //< Slot that next push will write in.
		int conspointer; //< Slot that next pop will return.
		volatile bool rundown;

		inline bool isFull()
		{
			return (prodpointer == ((conspointer + size - 1) % size));
		}

		inline bool isEmpty()
		{
			return (prodpointer == conspointer);
		}

		inline void incrementProd()
		{
			prodpointer = (prodpointer + 1) % size;
		}

		inline void incrementCons()
		{
			conspointer = (conspointer + 1) % size;
		}
};

#include "parallelqueue.inl"
