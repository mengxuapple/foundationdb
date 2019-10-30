/*
 * AtomicAdd.actor.cpp
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

// Workload description:
// AtomicAdd continuously increase a counter from 0;
// For each counter value, it writes a single key (ops/clientId/counter) by using atomicAdd to add the counter value;
// The result should be a key space (ops/clientId/, ops/clientId/1) with continuous keys whose values are equal to the counter field value.
// Example, (ops/1/2, 2), where 1 is the clientId and 2 is the counter value.
// The existance of this kv also means we must have kv pairs (ops/1/1, 1) and (ops/1/0, 0).

struct AtomicAddWorkload : TestWorkload {
	int actorCount, nodeCount;
	uint32_t opType;
	bool apiVersion500 = false;

	double testDuration, transactionsPerSecond;
	vector<Future<Void>> clients;

	int64_t maxCount;
	int64_t sum;
	int64_t max;

	AtomicAddWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		transactionsPerSecond = getOption( options, LiteralStringRef("transactionsPerSecond"), 5000.0 ) / clientCount;
		actorCount = getOption( options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5 );
		actorCount = 1; // The current implementation assumes actorCount is always 1.
		opType = MutationRef::AddValue;
		nodeCount = getOption( options, LiteralStringRef("nodeCount"), 1000 );
		maxCount = getOption( options, LiteralStringRef("maxCount"), 1000000 );

		// We follows AtomicOps.actor.cpp to assume the same apiVersion requirement although it is not a must.
		// Atomic OPs Min and And have modified behavior from api version 510. Hence allowing testing for older version (500) with a 10% probability
		// Actual change of api Version happens in setup
		apiVersion500 = ((sharedRandomNumber % 10) == 0);
		TraceEvent("AtomicOpsApiVersion500").detail("ApiVersion500", apiVersion500);

		int64_t randNum = sharedRandomNumber / 10;

		sum = 0;
		max = 0;

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
			    timeout(atomicAddWorker(cx->clone(), this, actorCount / transactionsPerSecond), testDuration, Void()));
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

	ACTOR Future<Void> _setup( Database cx, AtomicAddWorkload* self ) {
		// Sanity check if ops keyspace has elements
		state ReadYourWritesTransaction tr1(cx);
		loop {
			try {
				Key begin(std::string("ops"));
				Standalone<RangeResultRef> ops =
				    wait(tr1.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY));
				if (!ops.empty()) {
					TraceEvent(SevError, "AtomicAddSetup")
					    .detail("LogKeySpace", "Not empty")
					    .detail("Result", ops.toString());
					for (auto& kv : ops) {
						TraceEvent(SevWarn, "AtomicAddSetup")
						    .detail("K", kv.key.toString())
						    .detail("V", kv.value.toString());
					}
				}
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}

		state int counter = 0;
		for(; counter < self->maxCount; counter++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					uint64_t intValue = 0;
					tr.set(StringRef(format("ops/%08x/%08x",self->clientId, counter)), StringRef((const uint8_t*) &intValue, sizeof(intValue)));
					wait( tr.commit() );
					break;
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
		}

		return Void();
	}

	ACTOR Future<Void> atomicAddWorker( Database cx, AtomicAddWorkload* self, double delay ) {
		state double lastTime = now();
		state int64_t counter = 0;
		state Version version;
		loop {
			if (counter >= self->maxCount) {
				break;
			}
			wait( poisson( &lastTime, delay ) );
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					state Key val = StringRef((const uint8_t*) &counter, sizeof(counter));
					int64_t tmpSum = self->sum + counter;
					state Key tmpSumVal = StringRef((const uint8_t*) &tmpSum, sizeof(tmpSum));
					int64_t zero = 0;
					state Key zeroVal = StringRef((const uint8_t*) &zero, sizeof(zero));
					state Key key(format("ops/%08x/%08x", self->clientId, counter));
					//Optional<Value> readVal = wait( tr.get(key) );
					// if (!readVal.present()) {
					// 	tr.set(key, zeroVal);
					// } else {
					// 	TraceEvent(SevError, "AtomicAddWorker").detail("ClientID", self->clientId).detail("Counter", counter).detail("KeyNotExist", key.toString());
					// }
					TraceEvent(SevError, "AtomicAddWorker").detail("ClientID", self->clientId).detail("Counter", counter).detail("Key", key.toString());
					tr.atomicOp(key, val, self->opType);
					tr.set(StringRef(format("sum/%08x/%08x", self->clientId, counter)), tmpSumVal);
					wait( tr.commit() );
					version = tr.getCommittedVersion();
					TraceEvent("AtomicAddWorker").detail("ClientID", self->clientId).detail("Counter", counter).detail("Max", self->max).detail("Sum", self->sum).detail("Key", key).detail("Version", version);
					self->max = std::max(self->max, counter);
					self->sum += counter;
					counter++;
					break;
				} catch( Error &e ) {
					state int errorCode = e.code();
					wait( tr.onError(e) );
					// Ensure the counter increases continuously. We will use this property to check the result's correctness
					if (errorCode == 1021) { // commit_unknown_result
						loop {
							state ReadYourWritesTransaction tr1(cx);
							try {
								Key key(format("ops/%08x/%08x", self->clientId, counter));
								Optional<Value> txnSucceeded = wait(tr1.get(key));
								if (txnSucceeded.present()) {
									self->max = std::max(self->max, counter);
									self->sum += counter;
									counter++;
								}
								break;
							} catch (Error &e1) {
								wait( tr1.onError(e1) );
							}
						}
					}	
				}
			}
		}

		return Void();
	}

	ACTOR Future<bool> _check( Database cx, AtomicAddWorkload* self ) {
		state int counter = 0;
		state bool ret = true;
		state bool keepChecking = true;
		TraceEvent("AtomicAddCheck").detail("Max", self->max).detail("Sum", self->sum);
		for(; keepChecking && counter <= self->max; counter++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					// Check each key-value is correct
					state Key key(format("ops/%08x/%08x", self->clientId, counter));
					state Key sumkey(format("sum/%08x/%08x", self->clientId, counter));
					state Optional<Value> val = wait( tr.get(key) );
					state Optional<Value> sumval = wait( tr.get(sumkey) );
					if (!val.present() && !sumval.present()) {
						TraceEvent("AtomicAddCheck").detail("ClietID", self->clientId).detail("Counter", counter).detail("Key", key).detail("KeyStr", key.toString());
						keepChecking = false;
						break;
					}
					if (!val.present() || !sumval.present()) {
						TraceEvent(SevError, "AtomicAddMissKey").detail("ClientID", self->clientId).detail("Counter", counter).detail("Key", key.toString()).detail("ValueExist", val.present()).detail("SumExist", sumval.present());
						ret = false;
					} else {
						uint64_t intValue = 0;
						memcpy(&intValue, val.get().begin(), val.get().size());
						TraceEvent("AtomicAddCheck").detail("Key", key.toString()).detail("Val", val.get().toString()).detail("ValInt", intValue);
						if (intValue != counter) {
							TraceEvent(SevError, "AtomicAddMismatchedKey").detail("ClientID", self->clientId).detail("Counter", counter).detail("Key", key.toString()).detail("Value", intValue);
							ret = false;
						}
					}
					break;
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
		}
		return ret;
	}
};

WorkloadFactory<AtomicAddWorkload> AtomicAddWorkloadFactory("AtomicAdd");
