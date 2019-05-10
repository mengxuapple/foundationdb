/*
 * RestoreRoleCommon.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/MutationList.h"

#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreLoader.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreMaster.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

class Database;
struct RestoreWorkerData;

// id is the id of the worker to be monitored
// This actor is used for both restore loader and restore applier
ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id) {
	wait( delay(0.1) ); // To avoid warning
	req.reply.send(RestoreCommonReply(id, req.cmdID));

	return Void();
}

// Restore Worker: collect restore role interfaces locally by reading the specific system keys
ACTOR Future<Void> _collectRestoreRoleInterfaces(Reference<RestoreRoleData> self, Database cx) {
    state Transaction tr(cx);
	//state Standalone<RangeResultRef> loaderAgentValues;
	//state Standalone<RangeResultRef> applierAgentValues;
	printf("[INFO][Worker] Node:%s Get the handleCollectRestoreRoleInterfaceRequest for all workers\n", self->describeNode().c_str());
	loop {
		try {
			self->clearInterfaces();
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Standalone<RangeResultRef> loaderAgentValues = wait( tr.getRange(restoreLoaderKeys, CLIENT_KNOBS->TOO_MANY) );
			state Standalone<RangeResultRef> applierAgentValues = wait( tr.getRange(restoreApplierKeys, CLIENT_KNOBS->TOO_MANY) );
			ASSERT(!loaderAgentValues.more);
			ASSERT(!applierAgentValues.more);
			// Save the loader and applier interfaces for the later operations
			if (loaderAgentValues.size()) {
				for(auto& it : loaderAgentValues) {
					RestoreLoaderInterface loaderInterf = BinaryReader::fromStringRef<RestoreLoaderInterface>(it.value, IncludeVersion());
					self->loadersInterf[loaderInterf.id()] = loaderInterf;
				}
			}
			if (applierAgentValues.size()) {
				for(auto& it : applierAgentValues) {
					RestoreApplierInterface applierInterf = BinaryReader::fromStringRef<RestoreApplierInterface>(it.value, IncludeVersion());
					self->appliersInterf[applierInterf.id()] = applierInterf;
					self->masterApplierInterf = applierInterf; // TODO: Set masterApplier in a more deterministic way
				}
			}
			//wait(tr.commit());
			self->printRestoreRoleInterfaces();
			break;
		} catch( Error &e ) {
			printf("[WARNING] Node:%s handleCollectRestoreRoleInterfaceRequest() transaction error:%s\n", self->describeNode().c_str(), e.what());
			wait( tr.onError(e) );
		}
		printf("[WARNING] Node:%s handleCollectRestoreRoleInterfaceRequest should always succeed in the first loop! Something goes wrong!\n", self->describeNode().c_str());
	};

    return Void();
} 

// Restore worker
// RestoreRoleData will be casted to RestoreLoaderData or RestoreApplierData based on its type
ACTOR Future<Void> handleCollectRestoreRoleInterfaceRequest(RestoreSimpleRequest req, Reference<RestoreRoleData> self, Database cx) {

	while (self->isInProgress(RestoreCommandEnum::Collect_RestoreRoleInterface)) {
		printf("[DEBUG] NODE:%s handleCollectRestoreRoleInterfaceRequest wait for 5s\n",  self->describeNode().c_str());
		wait(delay(5.0));
	}
	// Handle duplicate, assuming cmdUID is always unique for the same workload
	if ( self->isCmdProcessed(req.cmdID) ) {
		printf("[DEBUG] NODE:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		return Void();
	} 

	self->setInProgressFlag(RestoreCommandEnum::Collect_RestoreRoleInterface);

    wait( _collectRestoreRoleInterfaces(self, cx) );

	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
	self->processedCmd[req.cmdID] = 1;
	self->clearInProgressFlag(RestoreCommandEnum::Collect_RestoreRoleInterface);

	return Void();
 }



ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self) {
	// wait( delay(1.0) );
	printf("[Batch:%d] Node:%s Start...\n", req.batchID, self->describeNode().c_str());
	while (self->isInProgress(RestoreCommandEnum::Reset_VersionBatch)) {
		printf("[DEBUG] NODE:%s handleVersionBatchRequest wait for 5s\n",  self->describeNode().c_str());
		wait(delay(5.0));
	}

	// Handle duplicate, assuming cmdUID is always unique for the same workload
	if ( self->isCmdProcessed(req.cmdID) ) {
		printf("[DEBUG] NODE:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		return Void();
	} 

	self->setInProgressFlag(RestoreCommandEnum::Reset_VersionBatch);

	self->resetPerVersionBatch();
	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));

	self->processedCmd[req.cmdID] = 1;
	self->clearInProgressFlag(RestoreCommandEnum::Reset_VersionBatch);

	// This actor never returns. You may cancel it in master
	return Void();
}


//-------Helper functions
std::string getHexString(StringRef input) {
	std::stringstream ss;
	for (int i = 0; i<input.size(); i++) {
		if ( i % 4 == 0 )
			ss << " ";
		if ( i == 12 ) { //The end of 12bytes, which is the version size for value
			ss << "|";
		}
		if ( i == (12 + 12) ) { //The end of version + header
			ss << "@";
		}
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}

std::string getHexKey(StringRef input, int skip) {
	std::stringstream ss;
	for (int i = 0; i<skip; i++) {
		if ( i % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	ss << "||";

	//hashvalue
	ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[skip]; // [] operator moves the pointer in step of unit8
	ss << "|";

	// commitversion in 64bit
	int count = 0;
	for (int i = skip+1; i<input.size() && i < skip+1+8; i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	// part value
	count = 0;
	for (int i = skip+1+8; i<input.size(); i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}


void printMutationListRefHex(MutationListRef m, std::string prefix) {
	MutationListRef::Iterator iter = m.begin();
	for ( ;iter != m.end(); ++iter) {
		printf("%s mType:%04x param1:%s param2:%s param1_size:%d, param2_size:%d\n", prefix.c_str(), iter->type,
			   getHexString(iter->param1).c_str(), getHexString(iter->param2).c_str(), iter->param1.size(), iter->param2.size());
	}
	return;
}

//TODO: Print out the backup mutation log value. The backup log value (i.e., the value in the kv pair) has the following format
//version(12B)|mutationRef|MutationRef|....
//A mutationRef has the format: |type_4B|param1_size_4B|param2_size_4B|param1|param2.
//Note: The data is stored in little endian! You need to convert it to BigEndian so that you know how long the param1 and param2 is and how to format them!
void printBackupMutationRefValueHex(Standalone<StringRef> val_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		fprintf(stderr, "%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		if ( debug_verbose ) {
			printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
		}
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFFCheckRestoreRequestDoneErrorMX
			//printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			fprintf(stderr, "%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		if ( debug_verbose ) {
			printf("%s---DedodeBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
				   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);
		}

	}
	if ( debug_verbose ) {
		printf("----------------------------------------------------------\n");
	}
}

void printBackupLogKeyHex(Standalone<StringRef> key_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = key_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		fprintf(stderr, "%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			//printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		printf("%s---DedoceBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
			   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

	}
	printf("----------------------------------------------------------\n");
}

void printLowerBounds(std::vector<Standalone<KeyRef>> lowerBounds) {
	if ( debug_verbose == false )
		return;

	printf("[INFO] Print out %ld keys in the lowerbounds\n", lowerBounds.size());
	for (int i = 0; i < lowerBounds.size(); i++) {
		printf("\t[INFO][%d] %s\n", i, getHexString(lowerBounds[i]).c_str());
	}
}


void printApplierKeyRangeInfo(std::map<UID, Standalone<KeyRangeRef>>  appliers) {
	printf("[INFO] appliers num:%ld\n", appliers.size());
	int index = 0;
	for(auto &applier : appliers) {
		printf("\t[INFO][Applier:%d] ID:%s --> KeyRange:%s\n", index, applier.first.toString().c_str(), applier.second.toString().c_str());
	}
}
