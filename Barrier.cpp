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

#include "Barrier.h"
#define fatal(...)

PThreadLockCVBarrier::PThreadLockCVBarrier(int nThreads) {
  m_nThreads = nThreads;

  int ret;
  ret = pthread_mutex_init(&m_l_SyncLock, NULL);
  if(ret!=0) fatal("pthread_mutex_init failed at barrier creation.\n");

  ret = pthread_cond_init(&m_cv_SyncCV, NULL);
  if(ret!=0) fatal("pthread_cond_init failed at barrier creation.\n");

  m_nSyncCount = 0;
}

PThreadLockCVBarrier::PThreadLockCVBarrier() {
  int ret;
  ret = pthread_mutex_init(&m_l_SyncLock, NULL);
  if(ret!=0) fatal("pthread_mutex_init failed at barrier creation.\n");

  ret = pthread_cond_init(&m_cv_SyncCV, NULL);
  if(ret!=0) fatal("pthread_cond_init failed at barrier creation.\n");

  m_nSyncCount = 0;
  m_nThreads = 0;
}

void PThreadLockCVBarrier::init(int nThreads) {
  m_nThreads = nThreads;
}

PThreadLockCVBarrier::~PThreadLockCVBarrier() {
  pthread_mutex_destroy(&m_l_SyncLock);
  pthread_cond_destroy(&m_cv_SyncCV);
}

void PThreadLockCVBarrier::Arrive() {
  if( m_nThreads < 1 ) {
    fatal("Invalid number of threads for barrier: %i\n", m_nThreads );
  }

  pthread_mutex_lock(&m_l_SyncLock);
  m_nSyncCount++;
  if(m_nSyncCount == m_nThreads) {
    pthread_cond_broadcast(&m_cv_SyncCV);
    m_nSyncCount = 0;
  } else {
    pthread_cond_wait(&m_cv_SyncCV, &m_l_SyncLock);
  }

  pthread_mutex_unlock(&m_l_SyncLock);
  
}

