/*
 * AtomicOps.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct AtomicOpsWorkload : TestWorkload {
	int opNum, actorCount, nodeCount;
	uint32_t opType;
	bool apiVersion500 = false;

	double testDuration, transactionsPerSecond;
	vector<Future<Void>> clients;
	float lbsum, ubsum;

	AtomicOpsWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), opNum(0)
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		transactionsPerSecond = getOption( options, LiteralStringRef("transactionsPerSecond"), 5000.0 ) / clientCount;
		actorCount = getOption( options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5 );
		opType = getOption( options, LiteralStringRef("opType"), -1 );
		nodeCount = getOption( options, LiteralStringRef("nodeCount"), 1000 );
		// Atomic OPs Min and And have modified behavior from api version 510. Hence allowing testing for older version (500) with a 10% probability
		// Actual change of api Version happens in setup
		apiVersion500 = ((sharedRandomNumber % 10) == 0);
		TraceEvent("AtomicOpsApiVersion500").detail("ApiVersion500", apiVersion500);
		lbsum = 0;
		ubsum = 0;

		int64_t randNum = sharedRandomNumber / 10;
		if(opType == -1)
			opType = randNum % 8;

		switch(opType) {
		case 0:
			TEST(true); //Testing atomic AddValue
			opType = MutationRef::AddValue;
			break;
		case 1:
			TEST(true); //Testing atomic And
			opType = MutationRef::And;
			break;
		case 2:
			TEST(true); //Testing atomic Or
			opType = MutationRef::Or;
			break;
		case 3:
			TEST(true); //Testing atomic Xor
			opType = MutationRef::Xor;
			break;
		case 4:
			TEST(true); //Testing atomic Max
			opType = MutationRef::Max;
			break;
		case 5:
			TEST(true); //Testing atomic Min
			opType = MutationRef::Min;
			break;
		case 6:
			TEST(true); //Testing atomic ByteMin
			opType = MutationRef::ByteMin;
			break;
		case 7:
			TEST(true); //Testing atomic ByteMax
			opType = MutationRef::ByteMax;
			break;
		default:
			ASSERT(false);
		}
		TraceEvent("AtomicWorkload").detail("OpType", opType);
	}

	virtual std::string description() { return "AtomicOps"; }

	virtual Future<Void> setup( Database const& cx ) {
		if (apiVersion500)
			cx->apiVersion = 500;

		if(clientId != 0)
			return Void();
		return _setup( cx, this );
	}

	virtual Future<Void> start( Database const& cx ) {
		for (int c = 0; c < actorCount; c++) {
			clients.push_back(
			    timeout(atomicOpWorker(cx->clone(), this, actorCount / transactionsPerSecond), testDuration, Void()));
		}

		return delay(testDuration);
	}

	virtual Future<bool> check( Database const& cx ) {
		if(clientId != 0)
			return true;
		return _check( cx, this );
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	Key logKey( int group ) { return StringRef(format("log%08x%08x%08x",group,clientId,opNum++));}

	ACTOR Future<Void> _setup( Database cx, AtomicOpsWorkload* self ) {
		// Sanity check if log keyspace has elements
		state ReadYourWritesTransaction tr1(cx);
		loop {
			try {
				Key begin(std::string("log"));
				Standalone<RangeResultRef> log =
				    wait(tr1.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY));
				if (!log.empty()) {
					TraceEvent(SevError, "AtomicOpSetup")
					    .detail("LogKeySpace", "Not empty")
					    .detail("Result", log.toString());
					for (auto& kv : log) {
						TraceEvent(SevWarn, "AtomicOpSetup")
						    .detail("K", kv.key.toString())
						    .detail("V", kv.value.toString());
					}
				}
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}

		state int g = 0;
		for(; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					for(int i = 0; i < self->nodeCount/100; i++) {
						uint64_t intValue = 0;
						tr.set(StringRef(format("ops%08x%08x",g,i)), StringRef((const uint8_t*) &intValue, sizeof(intValue)));
					}
					wait( tr.commit() );
					break;
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> atomicOpWorker( Database cx, AtomicOpsWorkload* self, double delay ) {
		state double lastTime = now();
		loop {
			wait( poisson( &lastTime, delay ) );
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					int group = deterministicRandom()->randomInt(0,100);
					state uint64_t intValue = deterministicRandom()->randomInt( 0, 10000000 );
					Key val = StringRef((const uint8_t*) &intValue, sizeof(intValue));
					tr.set(self->logKey(group), val);
					int nodeIndex = deterministicRandom()->randomInt(0, self->nodeCount / 100);
					tr.atomicOp(StringRef(format("ops%08x%08x", group, nodeIndex)), val, self->opType);
					wait( tr.commit() );
					lbsum += intValue;
					ubsum += intValue;
					break;
				} catch( Error &e ) {
					wait( tr.onError(e) );
					ubsum += intValue;
				}
			}
		}
	}

	ACTOR Future<bool> _check( Database cx, AtomicOpsWorkload* self ) {
		state int g = 0;
		state bool ret = true;
		for(; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			state Standalone<RangeResultRef> log;
			loop {
				try {
					{
						// Calculate the accumulated value in the log keyspace for the group g
						Key begin(format("log%08x", g));
						Standalone<RangeResultRef> log_ = wait( tr.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY) );
						log = log_;
						uint64_t zeroValue = 0;
						tr.set(LiteralStringRef("xlogResult"), StringRef((const uint8_t*) &zeroValue, sizeof(zeroValue)));
						for(auto& kv : log) {
							uint64_t intValue = 0;
							memcpy(&intValue, kv.value.begin(), kv.value.size());
							tr.atomicOp(LiteralStringRef("xlogResult"), kv.value, self->opType);
						}
					}

					{
						// Calculate the accumulated value in the ops keyspace for the group g
						Key begin(format("ops%08x", g));
						Standalone<RangeResultRef> ops = wait( tr.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY) );
						uint64_t zeroValue = 0;
						tr.set(LiteralStringRef("xopsResult"), StringRef((const uint8_t*) &zeroValue, sizeof(zeroValue)));
						for(auto& kv : ops) {
							uint64_t intValue = 0;
							memcpy(&intValue, kv.value.begin(), kv.value.size());
							tr.atomicOp(LiteralStringRef("xopsResult"), kv.value, self->opType);
						}

						if(tr.get(LiteralStringRef("xlogResult")).get() != tr.get(LiteralStringRef("xopsResult")).get()) {
							Optional<Standalone<StringRef>> logResult = tr.get(LiteralStringRef("xlogResult")).get();
							Optional<Standalone<StringRef>> opsResult = tr.get(LiteralStringRef("xopsResult")).get();
							ASSERT(logResult.present());
							ASSERT(opsResult.present());
							TraceEvent(SevError, "LogMismatch")
							    .detail("Index", format("log%08x", g))
							    .detail("LogResult", printable(logResult))
							    .detail("OpsResult", printable(opsResult));
						}

						if( self->opType == MutationRef::AddValue ) {
							uint64_t opsResult=0;
							Key opsResultStr = tr.get(LiteralStringRef("xopsResult")).get().get();
							memcpy(&opsResult, opsResultStr.begin(), opsResultStr.size());
							uint64_t logResult=0;
							for(auto& kv : log) {
								uint64_t intValue = 0;
								memcpy(&intValue, kv.value.begin(), kv.value.size());
								logResult += intValue;
							}
							if(logResult != opsResult) {
								TraceEvent(SevError, "LogAddMismatch").detail("LogResult", logResult).detail("OpResult", opsResult).detail("OpsResultStr", printable(opsResultStr)).detail("Size", opsResultStr.size())
									.detail("LowerBoundSum", seft->lbsum).detail("UperBoundSum", self->ubsum);
							}
						}
						break;
					}
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
		}
		return ret;
	}
};

WorkloadFactory<AtomicOpsWorkload> AtomicOpsWorkloadFactory("AtomicOps");
