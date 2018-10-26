/*
 * Restore.actor.cpp
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


#include "NativeAPI.h"
#include "SystemData.h"

//Backup agent header
#include "BackupAgent.h"
#include "RestoreInterface.h"
#include "FileBackupAgent.h"
#include "ManagementAPI.h"

/*
#include "BackupContainer.h"
#include "DatabaseContext.h"
#include "ManagementAPI.h"
#include "Status.h"
#include "KeyBackedTypes.h"
*/


#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>
//Backup agent header end

#include "flow/actorcompiler.h"  // This must be the last #include.

#define MX_DEBUG 1


//for convenience
typedef FileBackupAgent::ERestoreState ERestoreState;

template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val); // { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val); // { return (ERestoreState)val.getInt(0); }


static Future<Version> restoreMX(Database const &cx, RestoreRequest const &request);

//Future<Void> restoreAgentDB(Database cx, LocalityData const& locality)
ACTOR Future<Void>  restoreAgentDB(Database cx_input, LocalityData locality) {

	state Database cx = cx_input;
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: restoreAgent starts. Try to be a leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentStartTryBeLeader").detail("Locality", locality.toString());
	state Transaction tr(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr.numErrors);
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr.numErrors)
				.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr.commit());
			break;
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader. Locality:%s\n", locality.toString().c_str());
		TraceEvent("RestoreAgentNotLeader").detail("Locality", locality.toString());
		loop {
			try {
				tr.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr.commit());
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}

		loop {
			choose {
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}
	}

	//we are the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentIsLeader").detail("Locality", locality.toString());
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	state int testData = 0;
	loop {
		wait(delay(1.0));
		printf("Sending Request: %d\n", testData);
		TraceEvent("RestoreAgentLeader").detail("SendingRequest", testData);
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		printf("Wait on all %d requests for testData %d\n", agents.size(), testData);

		std::vector<TestReply> reps = wait( getAll(replies )); //When agent or master dies, we may stuck here. This is why we have not handled fault
		printf("getTestReply number:%d\n", reps.size());
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
		testData = reps[0].replyData;
	}

}

//MX: Hack: directly copy the function above with minor change
ACTOR Future<Void> restoreAgent(Reference<ClusterConnectionFile> ccf, LocalityData locality) {


	TraceEvent("RestoreAgent").detail("ClusterFile", ccf->getFilename());
	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1); //Cannot use filename to create cluster because the filename may not be found in the simulator when the function is invoked.
	Reference<Cluster> cluster = Cluster::createCluster(ccf, -1);
	state Database cx = wait(cluster->createDatabase(locality));
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: restoreAgent starts. Try to be a leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentStartTryBeLeader").detail("Locality", locality.toString());
	state Transaction tr(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr.numErrors);
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr.numErrors)
					.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr.commit());
			break;
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader. Locality:%s\n", locality.toString().c_str());
		TraceEvent("RestoreAgentNotLeader").detail("Locality", locality.toString());
		loop {
			try {
				tr.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr.commit());
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}

		loop {
			choose {
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}
	}

	//we are the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader. Locality:%s\n", locality.toString().c_str());
	TraceEvent("RestoreAgentIsLeader").detail("Locality", locality.toString());
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			TraceEvent(SevError, "GetRestoreAgentsKeysErrorMX");
			wait( tr.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	state int testData = 0;
	loop {
		wait(delay(1.0));
		printf("Sending Request: %d\n", testData);
		TraceEvent("RestoreAgentLeader").detail("SendingRequest", testData);
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		printf("Wait on all %d requests for testData %d\n", agents.size(), testData);

		std::vector<TestReply> reps = wait( getAll(replies ));
		printf("getTestReply number:%d\n", reps.size());
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
		testData = reps[0].replyData;
	}
}


//MX: This is where the restore code goes
//MX: Hack: directly copy the function above with minor change
//ACTOR Future<Void> restoreAgent_run(Database db) {
//ACTOR Future<Void> restoreAgent_run(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
ACTOR Future<Void> restoreAgent_runDB(Database cx_input, LocalityData locality) {

	//TraceEvent("RestoreAgentRun");
//	TraceEvent("RestoreAgentRun").detail("ClusterFile", ccf->getFilename());
//	//Reference<Cluster> cluster = Cluster::createCluster(ccf->getFilename(), -1); //Cannot use filename to create cluster because the filename may not be found in the simulator when the function is invoked.
//	Reference<Cluster> cluster = Cluster::createCluster(ccf, -1);
//	state Database cx = wait(cluster->createDatabase(locality));
	state Database cx = cx_input;

	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	printf("MX: RestoreAgentRun starts. Try to be a leader.\n");
	TraceEvent("RestoreAgentStartTryBeLeader");
	state Transaction tr0(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr0.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr0.numErrors).detail("ReadLeaderKey", restoreLeaderKey.printable());
			Optional<Value> leader = wait(tr0.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr0.numErrors)
					.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr0.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr0.commit());
			break;
		} catch( Error &e ) {
			wait( tr0.onError(e) );
		}
	}

	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader.\n");
		TraceEvent("RestoreAgentNotLeader");
		loop {
			try {
				tr0.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr0.commit());
				break;
			} catch( Error &e ) {
				wait( tr0.onError(e) );
			}
		}

		loop {
			choose {
				//Actual restore code
				when(RestoreRequest req = waitNext(interf.request.getFuture())) {
					printf("Got Restore Request: Index %d. RequestData:%d\n", req.testData, req.restoreRequests[req.testData]);
					TraceEvent("RestoreAgentNotLeader").detail("GotRestoreRequestID", req.testData)
						.detail("GotRestoreRequestValue", req.restoreRequests[req.testData]);
					//TODO: MX: actual restore
					std::vector<int> values(req.restoreRequests);
					values[req.testData]++;
					req.reply.send(RestoreReply(req.testData * -1, values));
					printf("Send Reply: %d, Value:%d\n", req.testData, values[req.testData]);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData).detail("Value", req.restoreRequests[req.testData]);
					if ( values[req.testData] > 10 ) { //MX: only calculate up to 10
						return Void();
					}
				}
				//Example code
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("GotRequest", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					printf("Send Reply: %d\n", req.testData);
					TraceEvent("RestoreAgentNotLeader").detail("SendReply", req.testData);
				}
			}
		}

	}

	//I am the leader
	//NOTE: The leader may be blocked when one agent dies. It will keep waiting for reply from the agents
	printf("MX: I am the leader.\n");
	TraceEvent("RestoreAgentIsLeader");
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	//state Transaction tr0(cx);
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr0.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(decodeRestoreAgentValue(it.value));
				}
				break;
			}
			printf("MX: agents number:%d\n", agentValues.size());
			TraceEvent("RestoreAgentLeader").detail("AgentSize", agentValues.size());
			wait( delay(5.0) );
		} catch( Error &e ) {
			TraceEvent("RestoreAgentLeaderErrorTr1").detail("ReadrestoreAgentsKeysFails", e.code()).detail("ErrorName", e.name());
			wait( tr0.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	//create initial point, The initial request value for agent i is i.
	state std::vector<int> restoreRequestsInt;
	for ( int i = 0; i < agents.size(); ++i ) {
		restoreRequestsInt.push_back(i);
	}

	// ----------------Restore code START
	state int restoreId = 0;
	loop {
		state vector<RestoreRequest> restoreRequests;
		loop {
			state Transaction tr2(cx);
			try {
				TraceEvent("CheckRestoreRequestTrigger");
				state Optional<Value> numRequests = wait(tr2.get(restoreRequestTriggerKey));
				if ( !numRequests.present() ) { // restore has not been triggered yet
					TraceEvent("CheckRestoreRequestTrigger").detail("SecondsOfWait", 5);
					wait( delay(5.0) );
					continue;
				}
				int num = decodeRestoreRequestTriggerValue(numRequests.get());
				TraceEvent("RestoreRequestKey").detail("NumRequests", num);
				Standalone<RangeResultRef> restoreRequestValues = wait(tr2.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!restoreRequestValues.more);

				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						restoreRequests.push_back(decodeRestoreRequestValue(it.value));
					}
					break;
				}
				break;
			} catch( Error &e ) {
				TraceEvent("RestoreAgentLeaderErrorTr2").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
				wait( tr2.onError(e) );
			}
		}
		// Perform the restore requests
		for ( auto &it : restoreRequests ) {
			TraceEvent("LeaderGotRestoreRequest").detail("RestoreRequestInfo", it.toString());
			Version ver = wait( restoreMX(cx, it) );
		}

		// Notify the finish of the restore by cleaning up the restore keys
		state Transaction tr3(cx);
		try {
			tr3.clear(restoreRequestTriggerKey);
			tr3.clear(restoreRequestKeys);
			tr3.set(restoreRequestDoneKey, restoreRequestDoneValue(restoreRequests.size()));
			TraceEvent("LeaderFinishRestoreRequest");
			wait(tr3.commit());
		}  catch( Error &e ) {
			TraceEvent("RestoreAgentLeaderErrorTr3").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
			wait( tr3.onError(e) );
		}

		TraceEvent("MXRestoreEndHere").detail("RestoreID", restoreId++);
		wait( delay(5.0) );
		//assert( 0 );
		//atomicRestoreMX();
	}


	// ----------------Restore code END


	// ---------------Increase counter example code
//	loop {
//		wait(delay(1.0));
//
//		printf("---Sending Requests\n");
//		TraceEvent("RestoreAgentLeader").detail("SendingRequests", "CheckBelow");
//		for (int i = 0; i < restoreRequestsInt.size(); ++i ) {
//			printf("RestoreRequests[%d]=%d\n", i, restoreRequestsInt[i]);
//			TraceEvent("RestoreRequests").detail("Index", i).detail("Value", restoreRequestsInt[i]);
//		}
//		std::vector<Future<RestoreReply>> replies;
//		for ( int i = 0; i < agents.size(); ++i) {
//			auto &it = agents[i];
//			replies.push_back( it.request.getReply(RestoreRequest(i, restoreRequestsInt)) );
//		}
//		printf("Wait on all %d requests\n", agents.size());
//
//		std::vector<RestoreReply> reps = wait( getAll(replies ));
//		printf("GetRestoreReply values\n", reps.size());
//		for ( int i = 0; i < reps.size(); ++i ) {
//			printf("RestoreReply[%d]=%d\n [checksum=%d]", i, reps[i].restoreReplies[i], reps[i].replyData);
//			//prepare to send the next request batch
//			restoreRequestsInt[i] = reps[i].restoreReplies[i];
//			if ( restoreRequestsInt[i] > 10 ) { //MX: only calculate up to 10
//				return Void();
//			}
//		}
//		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());
//	}
}

/*

// This method will return the final status of the backup
ACTOR static Future<ERestoreState> restoreAgentWaitRestore(Database cx, Key tagName, bool verbose) {
	loop {
		try {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state KeyBackedTag tag = makeRestoreTag(tagName.toString());
			Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
			if(!current.present()) {
				if(verbose)
					printf("Tag: %s  State: %s\n", tagName.toString().c_str(), FileBackupAgent::restoreStateText(ERestoreState::UNITIALIZED).toString().c_str());
				return ERestoreState::UNITIALIZED;
			}

			state RestoreConfig restore(current.get().first);

			if(verbose) {
				state std::string details = wait(restore.getProgress(tr));
				printf("%s\n", details.c_str());
			}

			state ERestoreState status = wait(restore.stateEnum().getD(tr));
			state bool runnable = wait(restore.isRunnable(tr));

			// State won't change from here
			if (!runnable)
				break;

			// Wait for a change
			state Future<Void> watchFuture = tr->watch(restore.stateEnum().key);
			wait(tr->commit());
			if(verbose)
				wait(watchFuture || delay(1));
			else
				wait(watchFuture);
		}
		catch (Error &e) {
			wait(tr->onError(e));
		}
	}

	return status;
}

*/


//-------------------------CODE FOR RESTORE----------------------------

ACTOR static Future<Void> _executeMX(Database cx,  Reference<Task> task) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state RestoreConfig restore(task);
	state Version restoreVersion;
	state Reference<IBackupContainer> bc;

	TraceEvent("ExecuteMX");

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			//wait(checkTaskVersion(tr->getDatabase(), task, name, version));
			Version _restoreVersion = wait(restore.restoreVersion().getOrThrow(tr)); //Failed
			restoreVersion = _restoreVersion;
			TraceEvent("ExecuteMX").detail("RestoreVersion", restoreVersion);
			//wait(taskBucket->keepRunning(tr, task));

			ERestoreState oldState = wait(restore.stateEnum().getD(tr));
			TraceEvent("ExecuteMX").detail("OldState", oldState);
			if(oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
				wait(restore.logError(cx, restore_error(), format("StartFullRestore: Encountered unexpected state(%d)", oldState), this));
				TraceEvent("StartFullRestoreMX").detail("Error", "Encounter unexpected state");
				return Void();
			}
			restore.stateEnum().set(tr, ERestoreState::STARTING);
			TraceEvent("ExecuteMX").detail("StateEnum", "Done");
			restore.fileSet().clear(tr);
			restore.fileBlockCount().clear(tr);
			restore.fileCount().clear(tr);
			TraceEvent("ExecuteMX").detail("Clear", "Done");
			Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
			TraceEvent("ExecuteMX").detail("BackupContainer", "Done");
			bc = _bc;

			wait(tr->commit());
			break;
		} catch(Error &e) {
			TraceEvent("ExecuteMXErrorTr").detail("ErrorName", e.name());
			wait(tr->onError(e));
			TraceEvent("ExecuteMXErrorTrDone");
		}
	}

	TraceEvent("ExecuteMX").detail("GetRestoreSet", restoreVersion);

	//MX: Get restore file set from BackupContainer
	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(restoreVersion));

	TraceEvent("ExecuteMX").detail("Restorable", restorable.present());

	if(!restorable.present())
		throw restore_missing_data();

	// First version for which log data should be applied
//	Params.firstVersion().set(task, restorable.get().snapshot.beginVersion);

	// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
	// Order does not matter, they will be put in order when written to the restoreFileMap below.
	state std::vector<RestoreConfig::RestoreFile> files;

	for(const RangeFile &f : restorable.get().ranges) {
		TraceEvent("FoundRangeFileMX").detail("FileInfo", f.toString());
		files.push_back({f.version, f.fileName, true, f.blockSize, f.fileSize});
	}
	for(const LogFile &f : restorable.get().logs) {
		TraceEvent("FoundLogFileMX").detail("FileInfo", f.toString());
		files.push_back({f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion});
	}

	state std::vector<RestoreConfig::RestoreFile>::iterator start = files.begin();
	state std::vector<RestoreConfig::RestoreFile>::iterator end = files.end();

	tr->reset();
	while(start != end) {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			//wait(taskBucket->keepRunning(tr, task));

			state std::vector<RestoreConfig::RestoreFile>::iterator i = start;

			state int txBytes = 0;
			state int nFileBlocks = 0;
			state int nFiles = 0;
			auto fileSet = restore.fileSet();
			for(; i != end && txBytes < 1e6; ++i) {
				txBytes += fileSet.insert(tr, *i);
				nFileBlocks += (i->fileSize + i->blockSize - 1) / i->blockSize;
				++nFiles;
			}

			// Increment counts
			restore.fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
			restore.fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

			wait(tr->commit());

			TraceEvent("FileRestoreLoadedFilesMX")
					.detail("RestoreUID", restore.getUid())
					.detail("FileCount", nFiles)
					.detail("FileBlockCount", nFileBlocks)
					.detail("TransactionBytes", txBytes)
					.detail("TaskInstance", (uint64_t)this);

			start = i;
			tr->reset();
		} catch(Error &e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR static Future<Void> submitRestoreMX(Database cx, Reference<ReadYourWritesTransaction> tr, Key tagName, Key backupURL, Version restoreVersion, Key addPrefix, Key removePrefix, KeyRange restoreRange, bool lockDB, UID uid,
		Reference<Task> task) {
	ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	// Get old restore config for this tag
	state KeyBackedTag tag = makeRestoreTag(tagName.toString());
	state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
	TraceEvent("SubmitRestoreMX").detail("OldUidAndAbortedPresent", oldUidAndAborted.present());
	if(oldUidAndAborted.present()) {
		if (oldUidAndAborted.get().first == uid) {
			if (oldUidAndAborted.get().second) {
				throw restore_duplicate_uid();
			}
			else {
				return Void();
			}
		}

		state RestoreConfig oldRestore(oldUidAndAborted.get().first);

		// Make sure old restore for this tag is not runnable
		bool runnable = wait(oldRestore.isRunnable(tr));

		if (runnable) {
			throw restore_duplicate_tag();
		}

		// Clear the old restore config
		oldRestore.clear(tr);
	}

	KeyRange restoreIntoRange = KeyRangeRef(restoreRange.begin, restoreRange.end).removePrefix(removePrefix).withPrefix(addPrefix);
	Standalone<RangeResultRef> existingRows = wait(tr->getRange(restoreIntoRange, 1));
	if (existingRows.size() > 0) {
		throw restore_destination_not_empty();
	}

	// Make new restore config
	state RestoreConfig restore(uid);

	// Point the tag to the new uid
	tag.set(tr, {uid, false});

	Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString());

	// Configure the new restore
	restore.tag().set(tr, tagName.toString());
	restore.sourceContainer().set(tr, bc);
	restore.stateEnum().set(tr, ERestoreState::QUEUED);
	restore.restoreVersion().set(tr, restoreVersion);
	restore.restoreRange().set(tr, restoreRange);
	// this also sets restore.add/removePrefix.
	restore.initApplyMutations(tr, addPrefix, removePrefix);
	TraceEvent("SubmitRestoreMX").detail("RestoreConfigConstruct", "Done");
	wait(restore.toTask(tr, task));

	// MX: no need to add task. Instead, we should directly run the execute function
	//Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

	if (lockDB)
		wait(lockDatabase(tr, uid));
	else
		wait(checkDatabaseLock(tr, uid));

	TraceEvent("RestoreMX").detail("UnlockDB", "Start");

	// MX: Unlock DB after restore
	if (lockDB)
		wait(unlockDatabase(tr, uid));
	else
		wait(checkDatabaseLock(tr, uid));

	return Void();
}


ACTOR static Future<Version> restoreMX(Database cx, RestoreRequest request) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange range = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;

	//Future<Void> restoreAgentFuture1 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());
	//Future<Void> restoreAgentFuture2 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());
	//TraceEvent("WaitOnRestoreAgentFutureBegin").detail("URL", url.contents().printable());
	//wait(restoreAgentFuture1 || restoreAgentFuture2);
	//TraceEvent("WaitOnRestoreAgentFutureEnd").detail("URL", url.contents().printable());

	state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString());
	state BackupDescription desc = wait(bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("Backup Description\n%s", desc.toString().c_str());
	printf("MX: Restore code comes here\n");
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));

	//Above is the restore master code
	//Below is the agent code
	TraceEvent("RestoreMX").detail("StartRestoreForRequest", request.toString());

	if(!restoreSet.present()) {
		TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
				.detail("BackupContainer", bc->getURL())
				.detail("TargetVersion", targetVersion);
		fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
		throw restore_invalid_version();
	}

	if (verbose) {
		printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		TraceEvent("RestoreBackupMX").detail("TargetVersion", (long long) targetVersion);
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Reference<Task> task(new Task());
			wait(submitRestoreMX(cx, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid, task));
			wait(tr->commit());
			// MX: Now execute the restore: Step 1 get the restore files (range and mutation log) name
			wait( _executeMX(cx, task) );
			break;
		} catch(Error &e) {
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
		}
	}

	//TODO: _finish() task: Make sure the restore is finished.

	//TODO: Uncomment the following code later
//	if(waitForComplete) {
//		ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
//		if(finalState != ERestoreState::COMPLETED)
//			throw restore_error();
//	}

	return targetVersion;
}

ACTOR static Future<Void> _executeApplyRangeFileToDB(Database cx, Reference<Task> task,
		RestoreFile rangeFile_input, int64_t readOffset_input, int64_t readLen_input) {

	state RestoreConfig restore(task);

	state RestoreFile rangeFile = rangeFile_input;
	state int64_t readOffset = readOffset_input;
	state int64_t readLen = readLen_input;


//	state RestoreFile rangeFile = Params.inputFile().get(task);
//	state int64_t readOffset = Params.readOffset().get(task);
//	state int64_t readLen = Params.readLen().get(task);

	TraceEvent("FileRestoreRangeStart")
			.suppressFor(60)
			.detail("RestoreUID", restore.getUid())
			.detail("FileName", rangeFile.fileName)
			.detail("FileVersion", rangeFile.version)
			.detail("FileSize", rangeFile.fileSize)
			.detail("ReadOffset", readOffset)
			.detail("ReadLen", readLen)
			.detail("TaskInstance", (uint64_t)this);

	state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
	state Future<Reference<IBackupContainer>> bc;
	state Future<KeyRange> restoreRange;
	state Future<Key> addPrefix;
	state Future<Key> removePrefix;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			bc = restore.sourceContainer().getOrThrow(tr);
			restoreRange = restore.restoreRange().getD(tr);
			addPrefix = restore.addPrefix().getD(tr);
			removePrefix = restore.removePrefix().getD(tr);

			//wait(taskBucket->keepRunning(tr, task));

			//wait(success(bc) && success(restoreRange) && success(addPrefix) && success(removePrefix) && checkTaskVersion(tr->getDatabase(), task, name, version));
			wait(success(bc) && success(restoreRange) && success(addPrefix) && success(removePrefix));
			break;

		} catch(Error &e) {
			wait(tr->onError(e));
		}
	}

	state Reference<IAsyncFile> inFile = wait(bc.get()->readFile(rangeFile.fileName));
	state Standalone<VectorRef<KeyValueRef>> blockData = wait(fileBackup::decodeRangeFileBlock(inFile, readOffset, readLen));
	TraceEvent("ApplyRangeFileToDB_MX").detail("BlockDataVectorSize", blockData.contents().size())
		.detail("RangeFirstKey", blockData.front().key.printable()).detail("RangeLastKey", blockData.back().key.printable());

	// First and last key are the range for this file
	state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

	// If fileRange doesn't intersect restore range then we're done.
	if(!fileRange.intersects(restoreRange.get())) {
		TraceEvent("ApplyRangeFileToDB_MX").detail("NoIntersectRestoreRange", "FinishAndReturn");
		return Void();
	}

	// We know the file range intersects the restore range but there could still be keys outside the restore range.
	// Find the subvector of kv pairs that intersect the restore range.  Note that the first and last keys are just the range endpoints for this file
	int rangeStart = 1;
	int rangeEnd = blockData.size() - 1;
	// Slide start forward, stop if something in range is found
	while(rangeStart < rangeEnd && !restoreRange.get().contains(blockData[rangeStart].key))
		++rangeStart;
	// Side end backward, stop if something in range is found
	while(rangeEnd > rangeStart && !restoreRange.get().contains(blockData[rangeEnd - 1].key))
		--rangeEnd;

	//MX: This is where the range file is splitted into smaller pieces
	state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

	// Shrink file range to be entirely within restoreRange and translate it to the new prefix
	// First, use the untranslated file range to create the shrunk original file range which must be used in the kv range version map for applying mutations
	state KeyRange originalFileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.get().begin), std::min(fileRange.end,   restoreRange.get().end));
//	Params.originalFileRange().set(task, originalFileRange);

	// Now shrink and translate fileRange
	Key fileEnd = std::min(fileRange.end,   restoreRange.get().end);
	if(fileEnd == (removePrefix.get() == StringRef() ? normalKeys.end : strinc(removePrefix.get())) ) {
		fileEnd = addPrefix.get() == StringRef() ? normalKeys.end : strinc(addPrefix.get());
	} else {
		fileEnd = fileEnd.removePrefix(removePrefix.get()).withPrefix(addPrefix.get());
	}
	fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.get().begin).removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),fileEnd);

	state int start = 0;
	state int end = data.size();
	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

	tr->reset();
	//MX: This is where the key-value pair in range file is applied into DB
	TraceEvent("ApplyRangeFileToDB_MX").detail("Progress", "StartApplyKVToDB").detail("DataSize", data.size()).detail("DataSizeLimit", dataSizeLimit);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state int i = start;
			state int txBytes = 0;
			state int iend = start;

			// find iend that results in the desired transaction size
			for(; iend < end && txBytes < dataSizeLimit; ++iend) {
				txBytes += data[iend].key.expectedSize();
				txBytes += data[iend].value.expectedSize();
			}

			// Clear the range we are about to set.
			// If start == 0 then use fileBegin for the start of the range, else data[start]
			// If iend == end then use fileEnd for the end of the range, else data[iend]
			state KeyRange trRange = KeyRangeRef((start == 0 ) ? fileRange.begin : data[start].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get())
					, (iend == end) ? fileRange.end   : data[iend ].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()));

			tr->clear(trRange);

			for(; i < iend; ++i) {
				tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr->set(data[i].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()), data[i].value);
			}

			// Add to bytes written count
			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

			state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

//			wait(taskBucket->keepRunning(tr, task));

			wait( checkLock );

			wait(tr->commit());

			TraceEvent("FileRestoreCommittedRange_MX")
					.suppressFor(60)
					.detail("RestoreUID", restore.getUid())
					.detail("FileName", rangeFile.fileName)
					.detail("FileVersion", rangeFile.version)
					.detail("FileSize", rangeFile.fileSize)
					.detail("ReadOffset", readOffset)
					.detail("ReadLen", readLen)
					.detail("CommitVersion", tr->getCommittedVersion())
					.detail("BeginRange", printable(trRange.begin))
					.detail("EndRange", printable(trRange.end))
					.detail("StartIndex", start)
					.detail("EndIndex", i)
					.detail("DataSize", data.size())
					.detail("Bytes", txBytes)
					.detail("OriginalFileRange", printable(originalFileRange))
					.detail("TaskInstance", (uint64_t)this);

			// Commit succeeded, so advance starting point
			start = i;

			if(start == end) {
				TraceEvent("ApplyRangeFileToDB_MX").detail("Progress", "DoneApplyKVToDB");
				return Void();
			}
			tr->reset();
		} catch(Error &e) {
			if(e.code() == error_code_transaction_too_large)
				dataSizeLimit /= 2;
			else
				wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> _executeApplyMutationLogFileToDB(Database cx, Reference<Task> task,
		RestoreFile logFile_input, int64_t readOffset_input, int64_t readLen_input) {
	state RestoreConfig restore(task);

	state RestoreFile logFile = logFile_input;
	state int64_t readOffset = readOffset_input;
	state int64_t readLen = readLen_input;

	TraceEvent("FileRestoreLogStart")
			.suppressFor(60)
			.detail("RestoreUID", restore.getUid())
			.detail("FileName", logFile.fileName)
			.detail("FileBeginVersion", logFile.version)
			.detail("FileEndVersion", logFile.endVersion)
			.detail("FileSize", logFile.fileSize)
			.detail("ReadOffset", readOffset)
			.detail("ReadLen", readLen)
			.detail("TaskInstance", (uint64_t)this);

	state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
	state Reference<IBackupContainer> bc;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
			bc = _bc;

//			wait(checkTaskVersion(tr->getDatabase(), task, name, version));
//			wait(taskBucket->keepRunning(tr, task));

			break;
		} catch(Error &e) {
			wait(tr->onError(e));
		}
	}

	state Key mutationLogPrefix = restore.mutationLogPrefix();
	state Reference<IAsyncFile> inFile = wait(bc->readFile(logFile.fileName));
	state Standalone<VectorRef<KeyValueRef>> data = wait(fileBackup::decodeLogFileBlock(inFile, readOffset, readLen));

	state int start = 0;
	state int end = data.size();
	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

	tr->reset();
	loop {
		try {
			if(start == end)
				return Void();

			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state int i = start;
			state int txBytes = 0;
			for(; i < end && txBytes < dataSizeLimit; ++i) {
				Key k = data[i].key.withPrefix(mutationLogPrefix);
				ValueRef v = data[i].value;
				tr->set(k, v);
				txBytes += k.expectedSize();
				txBytes += v.expectedSize();
			}

			state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

//			wait(taskBucket->keepRunning(tr, task));
			wait( checkLock );

			// Add to bytes written count
			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

			wait(tr->commit());

			TraceEvent("FileRestoreCommittedLog")
					.suppressFor(60)
					.detail("RestoreUID", restore.getUid())
					.detail("FileName", logFile.fileName)
					.detail("FileBeginVersion", logFile.version)
					.detail("FileEndVersion", logFile.endVersion)
					.detail("FileSize", logFile.fileSize)
					.detail("ReadOffset", readOffset)
					.detail("ReadLen", readLen)
					.detail("CommitVersion", tr->getCommittedVersion())
					.detail("StartIndex", start)
					.detail("EndIndex", i)
					.detail("DataSize", data.size())
					.detail("Bytes", txBytes)
					.detail("TaskInstance", (uint64_t)this);

			// Commit succeeded, so advance starting point
			start = i;
			tr->reset();
		} catch(Error &e) {
			if(e.code() == error_code_transaction_too_large)
				dataSizeLimit /= 2;
			else
				wait(tr->onError(e));
		}
	}
}

