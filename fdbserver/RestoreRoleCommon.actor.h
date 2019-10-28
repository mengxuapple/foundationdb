/*
 * RestoreRoleCommon.h
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

// This file delcares common struct and functions shared by restore roles, i.e.,
// RestoreMaster, RestoreLoader, RestoreApplier

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RestoreRoleCommon_G_H)
#define FDBSERVER_RestoreRoleCommon_G_H
#include "fdbserver/RestoreRoleCommon.actor.g.h"
#elif !defined(FDBSERVER_RestoreRoleCommon_H)
#define FDBSERVER_RestoreRoleCommon_H

#include <sstream>
#include "flow/Stats.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Notified.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreUtil.h"

#include "flow/actorcompiler.h" // has to be last include

extern bool debug_verbose;

struct RestoreRoleInterface;
struct RestoreLoaderInterface;
struct RestoreApplierInterface;

struct RestoreRoleData;
struct RestoreMasterData;

struct RestoreSimpleRequest;

typedef std::map<Version, Standalone<VectorRef<MutationRef>>> VersionedMutationsMap;

ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id);
void handleInitVersionBatchRequest(const RestoreVersionBatchRequest& req, Reference<RestoreRoleData> self);
void handleFinishRestoreRequest(const RestoreVersionBatchRequest& req, Reference<RestoreRoleData> self);

// Helper class for reading restore data from a buffer and throwing the right errors.
// This struct is mostly copied from StringRefReader. We add a sanity check in this struct.
// TODO: Merge this struct with StringRefReader.
struct StringRefReaderMX {
	StringRefReaderMX(StringRef s = StringRef(), Error e = Error())
	  : rptr(s.begin()), end(s.end()), failure_error(e), str_size(s.size()) {}

	// Return remainder of data as a StringRef
	StringRef remainder() { return StringRef(rptr, end - rptr); }

	// Return a pointer to len bytes at the current read position and advance read pos
	// Consume a little-Endian data. Since we only run on little-Endian machine, the data on storage is little Endian
	const uint8_t* consume(unsigned int len) {
		if (rptr == end && len != 0) throw end_of_stream();
		const uint8_t* p = rptr;
		rptr += len;
		if (rptr > end) {
			printf("[ERROR] StringRefReaderMX throw error! string length:%d\n", str_size);
			printf("!!!!!!!!!!!![ERROR]!!!!!!!!!!!!!! Worker may die due to the error. Master will stuck when a worker "
			       "die\n");
			throw failure_error;
		}
		return p;
	}

	// Return a T from the current read position and advance read pos
	template <typename T>
	const T consume() {
		return *(const T*)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte oselfer) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	const int32_t consumeNetworkInt32() { return (int32_t)bigEndian32((uint32_t)consume<int32_t>()); }
	const uint32_t consumeNetworkUInt32() { return bigEndian32(consume<uint32_t>()); }

	// Convert big Endian value (e.g., encoded in log file) into a littleEndian uint64_t value.
	const int64_t consumeNetworkInt64() { return (int64_t)bigEndian64((uint32_t)consume<int64_t>()); }
	const uint64_t consumeNetworkUInt64() { return bigEndian64(consume<uint64_t>()); }

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	const int str_size;
	Error failure_error;
};

// Restore Phase in each version batch
enum class RestorePhase {
	UNSET = 0,
	INIT,
	// setApplierKeyRangeVectorRequest
	SET_APPLIER_KEYRANGE,
	LOAD_LOG_FILE,
	LOAD_RANGE_FILE,
	APPLY_TO_DB,
	FINISH_RESTORE
};

struct RestoreRoleData : NonCopyable, public ReferenceCounted<RestoreRoleData> {
public:
	RestoreRole role;
	UID nodeID;
	int nodeIndex;

	std::map<UID, RestoreLoaderInterface> loadersInterf;
	std::map<UID, RestoreApplierInterface> appliersInterf;
	RestoreApplierInterface masterApplierInterf;

	NotifiedVersion versionBatchId; // Continuously increase for each versionBatch
	RestorePhase phase;

	bool versionBatchStart = false;

	uint32_t inProgressFlag = 0;

	RestoreRoleData() : role(RestoreRole::Invalid), phase(RestorePhase::UNSET){};

	virtual ~RestoreRoleData() {}

	UID id() const { return nodeID; }

	virtual void resetPerVersionBatch() = 0;

	void clearInterfaces() {
		loadersInterf.clear();
		appliersInterf.clear();
	}

	virtual std::string describeNode() = 0;
};

#include "flow/unactorcompiler.h"
#endif
