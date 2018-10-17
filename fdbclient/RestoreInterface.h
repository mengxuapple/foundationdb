/*
 * RestoreInterface.h
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

#ifndef FDBCLIENT_RestoreInterface_H
#define FDBCLIENT_RestoreInterface_H
#pragma once

#include "FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

struct RestoreInterface {
	RequestStream< struct TestRequest > test;
	RequestStream< struct RestoreRequest > request;

	bool operator == (RestoreInterface const& r) const { return id() == r.id(); }
	bool operator != (RestoreInterface const& r) const { return id() != r.id(); }
	UID id() const { return test.getEndpoint().token; }
	NetworkAddress address() const { return test.getEndpoint().address; }

	void initEndpoints() {
		test.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & test & request;
	}
};

struct TestRequest {
	int testData;
	ReplyPromise< struct TestReply > reply;

	TestRequest() : testData(0) {}
	explicit TestRequest(int testData) : testData(testData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & testData & reply;
	}
};

struct TestReply {
	int replyData;

	TestReply() : replyData(0) {}
	explicit TestReply(int replyData) : replyData(replyData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & replyData;
	}
};


struct RestoreRequest {
	int testData;
	std::vector<int> restoreRequests;
	//Key restoreTag;

	ReplyPromise< struct RestoreReply > reply;

	RestoreRequest() : testData(0) {}
	explicit RestoreRequest(int testData) : testData(testData) {}
	explicit RestoreRequest(int testData, std::vector<int> &restoreRequests) : testData(testData), restoreRequests(restoreRequests) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & testData & restoreRequests & reply;
	}
};

struct RestoreReply {
	int replyData;
	std::vector<int> restoreReplies;

	RestoreReply() : replyData(0) {}
	explicit RestoreReply(int replyData) : replyData(replyData) {}
	explicit RestoreReply(int replyData, std::vector<int> restoreReplies) : replyData(replyData), restoreReplies(restoreReplies) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & replyData & restoreReplies;
	}
};


class FileBackupAgent; //declare ahead
class RestoreConfig;

Future<Void> restoreAgent(Reference<struct ClusterConnectionFile> const& ccf, struct LocalityData const& locality);
Future<Void> restoreAgentDB(Database const& cx, LocalityData const& locality);
//Future<Void> restoreAgent_run(Database const &db);
Future<Void> restoreAgent_run(Reference<ClusterConnectionFile> const& ccf, LocalityData const& locality);
//Future<Version> restoreAgentRestore(FileBackupAgent const* backupAgent, Database const& cx, Key const& tagName, Key const& url,
//		bool waitForComplete, Version const& targetVersion, bool verbose, KeyRange const& range, Key const& addPrefix, Key const& removePrefix, bool const lockDB, UID const &randomUid);
Future<Void> restoreAgentRestore(FileBackupAgent* const& backupAgent, Database const& db, Standalone<StringRef> const& tagName,
        Standalone<StringRef> const& url, bool const& waitForComplete, Version const& targetVersion, bool const& verbose,
        Standalone<KeyRangeRef> const& range, Standalone<StringRef> const& addPrefix, Standalone<StringRef> const& removePrefix, bool const& lockDB, UID const& randomUid);

#endif
