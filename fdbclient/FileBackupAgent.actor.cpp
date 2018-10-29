/*
 * FileBackupAgent.actor.cpp
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

#include "BackupAgent.h"
#include "BackupContainer.h"
#include "DatabaseContext.h"
#include "ManagementAPI.h"
#include "Status.h"
#include "KeyBackedTypes.h"

#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

#include "RestoreInterface.h"
#include "FileBackupAgent.h"

#include "flow/actorcompiler.h"  // This must be the last #include.


static std::string boolToYesOrNo(bool val) { return val ? std::string("Yes") : std::string("No"); }

static std::string versionToString(Optional<Version> version) {
	if (version.present())
		return std::to_string(version.get());
	else
		return "N/A";
}

static std::string timeStampToString(Optional<int64_t> ts) {
	if (!ts.present())
		return "N/A";
	time_t curTs = ts.get();
	char buffer[128];
	struct tm* timeinfo;
	timeinfo = localtime(&curTs);
	strftime(buffer, 128, "%D %T", timeinfo);
	return std::string(buffer);
}

static Future<Optional<int64_t>> getTimestampFromVersion(Optional<Version> ver, Reference<ReadYourWritesTransaction> tr) {
	if (!ver.present())
		return Optional<int64_t>();

	return timeKeeperEpochsFromVersion(ver.get(), tr);
}

// Time format :
// <= 59 seconds
// <= 59.99 minutes
// <= 23.99 hours
// N.NN days
std::string secondsToTimeFormat(int64_t seconds) {
	if (seconds >= 86400)
		return format("%.2f day(s)", seconds / 86400.0);
	else if (seconds >= 3600)
		return format("%.2f hour(s)", seconds / 3600.0);
	else if (seconds >= 60)
		return format("%.2f minute(s)", seconds / 60.0);
	else
		return format("%ld second(s)", seconds);
}

const Key FileBackupAgent::keyLastRestorable = LiteralStringRef("last_restorable");



StringRef FileBackupAgent::restoreStateText(ERestoreState id) {
	switch(id) {
		case ERestoreState::UNITIALIZED: return LiteralStringRef("unitialized");
		case ERestoreState::QUEUED: return LiteralStringRef("queued");
		case ERestoreState::STARTING: return LiteralStringRef("starting");
		case ERestoreState::RUNNING: return LiteralStringRef("running");
		case ERestoreState::COMPLETED: return LiteralStringRef("completed");
		case ERestoreState::ABORTED: return LiteralStringRef("aborted");
		default: return LiteralStringRef("Unknown");
	}
}


ACTOR Future<std::vector<KeyBackedTag>> TagUidMap::getAll_impl(TagUidMap *tagsMap, Reference<ReadYourWritesTransaction> tr) {
	state Key prefix = tagsMap->prefix; // Copying it here as tagsMap lifetime is not tied to this actor
	TagMap::PairsType tagPairs = wait(tagsMap->getRange(tr, std::string(), {}, 1e6));
	std::vector<KeyBackedTag> results;
	for(auto &p : tagPairs)
		results.push_back(KeyBackedTag(p.first, prefix));
	return results;
}

KeyBackedTag::KeyBackedTag(std::string tagName, StringRef tagMapPrefix)
		: KeyBackedProperty<UidAndAbortedFlagT>(TagUidMap(tagMapPrefix).getProperty(tagName)), tagName(tagName), tagMapPrefix(tagMapPrefix) {}


Future<StringRef> RestoreConfig::stateText(Reference<ReadYourWritesTransaction> tr) {
	return map(stateEnum().getD(tr), [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
}


template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val) { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val) { return (ERestoreState)val.getInt(0); }


ACTOR Future<int64_t> RestoreConfig::getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
	// Both of these are snapshot reads
	state Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), true);
	state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), true);
	wait(success(beginVal) && success(endVal));

	if(!beginVal.get().present() || !endVal.get().present())
		return 0;

	Version beginVersion = BinaryReader::fromStringRef<Version>(beginVal.get().get(), Unversioned());
	Version endVersion = BinaryReader::fromStringRef<Version>(endVal.get().get(), Unversioned());
	return endVersion - beginVersion;
}


//typedef RestoreConfig::RestoreFile RestoreFile;

ACTOR Future<std::string> RestoreConfig::getProgress_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<int64_t> fileCount = restore.fileCount().getD(tr);
	state Future<int64_t> fileBlockCount = restore.fileBlockCount().getD(tr);
	state Future<int64_t> fileBlocksDispatched = restore.filesBlocksDispatched().getD(tr);
	state Future<int64_t> fileBlocksFinished = restore.fileBlocksFinished().getD(tr);
	state Future<int64_t> bytesWritten = restore.bytesWritten().getD(tr);
	state Future<StringRef> status = restore.stateText(tr);
	state Future<Version> lag = restore.getApplyVersionLag(tr);
	state Future<std::string> tag = restore.tag().getD(tr);
	state Future<std::pair<std::string, Version>> lastError = restore.lastError().getD(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore.getUid();
	wait(success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) && success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(lag) && success(tag) && success(lastError));

	std::string errstr = "None";
	if(lastError.get().second != 0)
		errstr = format("'%s' %llds ago.\n", lastError.get().first.c_str(), (tr->getReadVersion().get() - lastError.get().second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND );

	TraceEvent("FileRestoreProgress")
		.detail("RestoreUID", uid)
		.detail("Tag", tag.get())
		.detail("State", status.get().toString())
		.detail("FileCount", fileCount.get())
		.detail("FileBlocksFinished", fileBlocksFinished.get())
		.detail("FileBlocksTotal", fileBlockCount.get())
		.detail("FileBlocksInProgress", fileBlocksDispatched.get() - fileBlocksFinished.get())
		.detail("BytesWritten", bytesWritten.get())
		.detail("ApplyLag", lag.get())
		.detail("TaskInstance", (uint64_t)this);


	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: %lld  ApplyVersionLag: %lld  LastError: %s",
					tag.get().c_str(),
					uid.toString().c_str(),
					status.get().toString().c_str(),
					fileBlocksFinished.get(),
					fileBlockCount.get(),
					fileBlocksDispatched.get() - fileBlocksFinished.get(),
					fileCount.get(),
					bytesWritten.get(),
					lag.get(),
					errstr.c_str()
				);
}

ACTOR Future<std::string> RestoreConfig::getFullStatus_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<KeyRange> range = restore.restoreRange().getD(tr);
	state Future<Key> addPrefix = restore.addPrefix().getD(tr);
	state Future<Key> removePrefix = restore.removePrefix().getD(tr);
	state Future<Key> url = restore.sourceContainerURL().getD(tr);
	state Future<Version> restoreVersion = restore.restoreVersion().getD(tr);
	state Future<std::string> progress = restore.getProgress(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore.getUid();
	wait(success(range) && success(addPrefix) && success(removePrefix) && success(url) && success(restoreVersion) && success(progress));

	return format("%s  URL: %s  Begin: '%s'  End: '%s'  AddPrefix: '%s'  RemovePrefix: '%s'  Version: %lld",
					progress.get().c_str(),
					url.get().toString().c_str(),
					printable(range.get().begin).c_str(),
					printable(range.get().end).c_str(),
					printable(addPrefix.get()).c_str(),
					printable(removePrefix.get()).c_str(),
					restoreVersion.get()
	);
}


FileBackupAgent::FileBackupAgent()
	: subspace(Subspace(fileBackupPrefixRange.begin))
	// The other subspaces have logUID -> value
	, config(subspace.get(BackupAgentBase::keyConfig))
	, lastRestorable(subspace.get(FileBackupAgent::keyLastRestorable))
	, taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks), true, false, true))
	, futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), true, true))
{
}

namespace fileBackup {

	// Padding bytes for backup files.  The largest padded area that could ever have to be written is
	// the size of two 32 bit ints and the largest key size and largest value size.  Since CLIENT_KNOBS
	// may not be initialized yet a conservative constant is being used.
	std::string paddingFFs(128 * 1024, 0xFF);

	// File Format handlers.
	// Both Range and Log formats are designed to be readable starting at any 1MB boundary
	// so they can be read in parallel.
	//
	// Writer instances must be kept alive while any member actors are in progress.
	//
	// RangeFileWriter must be used as follows:
	//   1 - writeKey(key) the queried key range begin
	//   2 - writeKV(k, v) each kv pair to restore
	//   3 - writeKey(key) the queried key range end
	//
	// RangeFileWriter will insert the required padding, header, and extra
	// end/begin keys around the 1MB boundaries as needed.
	//
	// Example:
	//   The range a-z is queries and returns c-j which covers 3 blocks.
	//   The client code writes keys in this sequence:
	//             a c d e f g h i j z
	//
	//   H = header   P = padding   a...z = keys  v = value | = block boundary
	//
	//   Encoded file:  H a cv dv ev P | H e ev fv gv hv P | H h hv iv jv z
	//   Decoded in blocks yields:
	//           Block 1: range [a, e) with kv pairs cv, dv
	//           Block 2: range [e, h) with kv pairs ev, fv, gv
	//           Block 3: range [h, z) with kv pairs hv, iv, jv
	//
	//   NOTE: All blocks except for the final block will have one last
	//   value which will not be used.  This isn't actually a waste since
	//   if the next KV pair wouldn't fit within the block after the value
	//   then the space after the final key to the next 1MB boundary would
	//   just be padding anyway.
	struct RangeFileWriter {
		RangeFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0) : file(file), blockSize(blockSize), blockEnd(0), fileVersion(1001) {}

		// Handles the first block and internal blocks.  Ends current block if needed.
		ACTOR static Future<Void> newBlock(RangeFileWriter *self, int bytesNeeded) {
			// Write padding to finish current block if needed
			int bytesLeft = self->blockEnd - self->file->size();
			if(bytesLeft > 0) {
				wait(self->file->append((uint8_t *)paddingFFs.data(), bytesLeft));
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write Header
			wait(self->file->append((uint8_t *)&self->fileVersion, sizeof(self->fileVersion)));

			// If this is NOT the first block then write duplicate stuff needed from last block
			if(self->blockEnd > self->blockSize) {
				wait(self->file->appendStringRefWithLen(self->lastKey));
				wait(self->file->appendStringRefWithLen(self->lastKey));
				wait(self->file->appendStringRefWithLen(self->lastValue));
			}

			// There must now be room in the current block for bytesNeeded or the block size is too small
			if(self->file->size() + bytesNeeded > self->blockEnd)
				throw backup_bad_block_size();

			return Void();
		}

		// Ends the current block if necessary based on bytesNeeded.
		Future<Void> newBlockIfNeeded(int bytesNeeded) {
			if(file->size() + bytesNeeded > blockEnd)
				return newBlock(this, bytesNeeded);
			return Void();
		}

		// Start a new block if needed, then write the key and value
		ACTOR static Future<Void> writeKV_impl(RangeFileWriter *self, Key k, Value v) {
			int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
			wait(self->newBlockIfNeeded(toWrite));
			wait(self->file->appendStringRefWithLen(k));
			wait(self->file->appendStringRefWithLen(v));
			self->lastKey = k;
			self->lastValue = v;
			return Void();
		}

		Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

		// Write begin key or end key.
		ACTOR static Future<Void> writeKey_impl(RangeFileWriter *self, Key k) {
			int toWrite = sizeof(uint32_t) + k.size();
			wait(self->newBlockIfNeeded(toWrite));
			wait(self->file->appendStringRefWithLen(k));
			return Void();
		}

		Future<Void> writeKey(Key k) { return writeKey_impl(this, k); }

		Reference<IBackupFile> file;
		int blockSize;

	private:
		int64_t blockEnd;
		uint32_t fileVersion;
		Key lastKey;
		Key lastValue;
	};

	// Helper class for reading restore data from a buffer and throwing the right errors.
	struct StringRefReader {
		StringRefReader(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

		// Return remainder of data as a StringRef
		StringRef remainder() {
			return StringRef(rptr, end - rptr);
		}

		// Return a pointer to len bytes at the current read position and advance read pos
		const uint8_t * consume(unsigned int len) {
			if(rptr == end && len != 0)
				throw end_of_stream();
			const uint8_t *p = rptr;
			rptr += len;
			if(rptr > end)
				throw failure_error;
			return p;
		}

		// Return a T from the current read position and advance read pos
		template<typename T> const T consume() {
			return *(const T *)consume(sizeof(T));
		}

		// Functions for consuming big endian (network byte order) integers.
		// Consumes a big endian number, swaps it to little endian, and returns it.
		const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
		const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

		bool eof() { return rptr == end; }

		const uint8_t *rptr, *end;
		Error failure_error;
	};

	//MX: This is where the range file is decoded, broken into smaller pieces and applied to DB
	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 1001
			if(reader.consume<int32_t>() != 1001)
				throw restore_unsupported_file_version();

			// Read begin key, if this fails then block was invalid.
			uint32_t kLen = reader.consumeNetworkUInt32();
			const uint8_t *k = reader.consume(kLen);
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));

			// Read kv pairs and end key
			while(1) {
				// Read a key.
				kLen = reader.consumeNetworkUInt32();
				k = reader.consume(kLen);

				// If eof reached or first value len byte is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF) {
					results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
					break;
				}

				// Read a value, which must exist or the block is invalid
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);
				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

				// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevWarn, "FileRestoreCorruptRangeFileBlock")
				.error(e)
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
			throw;
		}
	}


	// Very simple format compared to KeyRange files.
	// Header, [Key, Value]... Key len
	struct LogFileWriter {
		static const std::string &FFs;

		LogFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0) : file(file), blockSize(blockSize), blockEnd(0), fileVersion(2001) {}

		// Start a new block if needed, then write the key and value
		ACTOR static Future<Void> writeKV_impl(LogFileWriter *self, Key k, Value v) {
			// If key and value do not fit in this block, end it and start a new one
			int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
			if(self->file->size() + toWrite > self->blockEnd) {
				// Write padding if needed
				int bytesLeft = self->blockEnd - self->file->size();
				if(bytesLeft > 0) {
					wait(self->file->append((uint8_t *)paddingFFs.data(), bytesLeft));
				}

				// Set new blockEnd
				self->blockEnd += self->blockSize;

				// write Header
				wait(self->file->append((uint8_t *)&self->fileVersion, sizeof(self->fileVersion)));
			}

			wait(self->file->appendStringRefWithLen(k));
			wait(self->file->appendStringRefWithLen(v));

			// At this point we should be in whatever the current block is or the block size is too small
			if(self->file->size() > self->blockEnd)
				throw backup_bad_block_size();

			return Void();
		}

		Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

		Reference<IBackupFile> file;
		int blockSize;

	private:
		int64_t blockEnd;
		uint32_t fileVersion;
	};

	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 2001
			if(reader.consume<int32_t>() != 2001)
				throw restore_unsupported_file_version();

			// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
			while(1) {
				// If eof reached or first key len bytes is 0xFF then end of block was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;

				// Read key and value.  If anything throws then there is a problem.
				uint32_t kLen = reader.consumeNetworkUInt32();
				const uint8_t *k = reader.consume(kLen);
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);

				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
				.error(e)
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
			throw;
		}
	}

	ACTOR Future<Void> checkTaskVersion(Database cx, Reference<Task> task, StringRef name, uint32_t version) {
		uint32_t taskVersion = task->getVersion();
		if (taskVersion > version) {
			state Error err = task_invalid_version();

			TraceEvent(SevWarn, "BA_BackupRangeTaskFuncExecute").detail("TaskVersion", taskVersion).detail("Name", printable(name)).detail("Version", version);
			if (KeyBackedConfig::TaskParams.uid().exists(task)) {
				std::string msg = format("%s task version `%lu' is greater than supported version `%lu'", task->params[Task::reservedTaskParamKeyType].toString().c_str(), (unsigned long)taskVersion, (unsigned long)version);
				wait(BackupConfig(task).logError(cx, err, msg));
			}

			throw err;
		}

		return Void();
	}

	ACTOR static Future<Void> abortFiveZeroBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, std::string tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state Subspace tagNames = backupAgent->subspace.get(BackupAgentBase::keyTagName);
		Optional<Value> uidStr = wait(tr->get(tagNames.pack(Key(tagName))));
		if (!uidStr.present()) {
			TraceEvent(SevWarn, "FileBackupAbortIncompatibleBackup_TagNotFound").detail("TagName", tagName.c_str());
			return Void();
		}
		state UID uid = BinaryReader::fromStringRef<UID>(uidStr.get(), Unversioned());

		state Subspace statusSpace = backupAgent->subspace.get(BackupAgentBase::keyStates).get(uid.toString());
		state Subspace globalConfig = backupAgent->subspace.get(BackupAgentBase::keyConfig).get(uid.toString());
		state Subspace newConfigSpace = uidPrefixKey(LiteralStringRef("uid->config/").withPrefix(fileBackupPrefixRange.begin), uid);

		Optional<Value> statusStr = wait(tr->get(statusSpace.pack(FileBackupAgent::keyStateStatus)));
		state EBackupState status = !statusStr.present() ? FileBackupAgent::STATE_NEVERRAN : BackupAgentBase::getState(statusStr.get().toString());

		TraceEvent(SevInfo, "FileBackupAbortIncompatibleBackup")
				.detail("TagName", tagName.c_str())
				.detail("Status", BackupAgentBase::getStateText(status));

		// Clear the folder id to prevent future tasks from executing at all
		tr->clear(singleKeyRange(StringRef(globalConfig.pack(FileBackupAgent::keyFolderId))));

		// Clear the mutations logging config and data
		Key configPath = uidPrefixKey(logRangesRange.begin, uid);
		Key logsPath = uidPrefixKey(backupLogKeys.begin, uid);
		tr->clear(KeyRangeRef(configPath, strinc(configPath)));
		tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

		// Clear the new-style config space
		tr->clear(newConfigSpace.range());

		Key statusKey = StringRef(statusSpace.pack(FileBackupAgent::keyStateStatus));

		// Set old style state key to Aborted if it was Runnable
		if(backupAgent->isRunnable(status))
			tr->set(statusKey, StringRef(FileBackupAgent::getStateText(BackupAgentBase::STATE_ABORTED)));

		return Void();
	}

	struct AbortFiveZeroBackupTask : TaskFuncBase {
		static StringRef name;
		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state FileBackupAgent backupAgent;
			state std::string tagName = task->params[BackupAgentBase::keyConfigBackupTag].toString();

			TEST(true);  // Canceling old backup task

			TraceEvent(SevInfo, "FileBackupCancelOldTask")
					.detail("Task", printable(task->params[Task::reservedTaskParamKeyType]))
					.detail("TagName", tagName);
			wait(abortFiveZeroBackup(&backupAgent, tr, tagName));

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		virtual StringRef getName() const {
			TraceEvent(SevError, "FileBackupError").detail("Cause", "AbortFiveZeroBackupTaskFunc::name() should never be called");
			ASSERT(false);
			return StringRef();
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Future<Void>(Void()); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef AbortFiveZeroBackupTask::name = LiteralStringRef("abort_legacy_backup");
	REGISTER_TASKFUNC(AbortFiveZeroBackupTask);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_diff_logs);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_log_range);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_logs);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_range);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_restorable);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finish_full_backup);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finished_full_backup);
	REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_start_full_backup);

	ACTOR static Future<Void> abortFiveOneBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, std::string tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName);
		state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, false, backup_unneeded()));

		state BackupConfig config(current.first);
		EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));

		if (!backupAgent->isRunnable((BackupAgentBase::enumState)status)) {
			throw backup_unneeded();
		}

		TraceEvent(SevInfo, "FBA_AbortFileOneBackup")
				.detail("TagName", tagName.c_str())
				.detail("Status", BackupAgentBase::getStateText(status));

		// Cancel backup task through tag
		wait(tag.cancel(tr));

		Key configPath = uidPrefixKey(logRangesRange.begin, config.getUid());
		Key logsPath = uidPrefixKey(backupLogKeys.begin, config.getUid());

		tr->clear(KeyRangeRef(configPath, strinc(configPath)));
		tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

		config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

		return Void();
	}

	struct AbortFiveOneBackupTask : TaskFuncBase {
		static StringRef name;
		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state FileBackupAgent backupAgent;
			state BackupConfig config(task);
			state std::string tagName = wait(config.tag().getOrThrow(tr));

			TEST(true);  // Canceling 5.1 backup task

			TraceEvent(SevInfo, "FileBackupCancelFiveOneTask")
					.detail("Task", printable(task->params[Task::reservedTaskParamKeyType]))
					.detail("TagName", tagName);
			wait(abortFiveOneBackup(&backupAgent, tr, tagName));

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		virtual StringRef getName() const {
			TraceEvent(SevError, "FileBackupError").detail("Cause", "AbortFiveOneBackupTaskFunc::name() should never be called");
			ASSERT(false);
			return StringRef();
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Future<Void>(Void()); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef AbortFiveOneBackupTask::name = LiteralStringRef("abort_legacy_backup_5.2");
	REGISTER_TASKFUNC(AbortFiveOneBackupTask);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_range);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_ranges);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_logs);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_erase_logs);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_logs);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_finished);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_snapshot_manifest);
	REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_start);

	std::function<void(Reference<Task>)> NOP_SETUP_TASK_FN = [](Reference<Task> task) { /* NOP */ };
	ACTOR static Future<Key> addBackupTask(StringRef name,
										   uint32_t version,
										   Reference<ReadYourWritesTransaction> tr,
										   Reference<TaskBucket> taskBucket,
										   TaskCompletionKey completionKey,
										   BackupConfig config,
										   Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
										   std::function<void(Reference<Task>)> setupTaskFn = NOP_SETUP_TASK_FN,
										   int priority = 0,
										   bool setValidation = true) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		Key doneKey = wait(completionKey.get(tr, taskBucket));
		state Reference<Task> task(new Task(name, version, doneKey, priority));

		// Bind backup config to new task
		wait(config.toTask(tr, task, setValidation));

		// Set task specific params
		setupTaskFn(task);

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}
		wait(waitFor->onSetAddTask(tr, taskBucket, task));

		return LiteralStringRef("OnSetAddTask");
	}

	// Backup and Restore taskFunc definitions will inherit from one of the following classes which
	// servers to catch and log to the appropriate config any error that execute/finish didn't catch and log.
	struct RestoreTaskFuncBase : TaskFuncBase {
		virtual Future<Void> handleError(Database cx, Reference<Task> task, Error const &error) {
			return RestoreConfig(task).logError(cx, error, format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
		}
		virtual std::string toString(Reference<Task> task)
		{
			return "";
		}
	};

	struct BackupTaskFuncBase : TaskFuncBase {
		virtual Future<Void> handleError(Database cx, Reference<Task> task, Error const &error) {
			return BackupConfig(task).logError(cx, error, format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
		}
		virtual std::string toString(Reference<Task> task)
		{
			return "";
		}
	};

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getBlockOfShards(Reference<ReadYourWritesTransaction> tr, Key beginKey, Key endKey, int limit) {

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Standalone<VectorRef<KeyRef>> results;
		Standalone<RangeResultRef> values = wait(tr->getRange(KeyRangeRef(keyAfter(beginKey.withPrefix(keyServersPrefix)), endKey.withPrefix(keyServersPrefix)), limit));

		for (auto &s : values) {
			KeyRef k = s.key.removePrefix(keyServersPrefix);
			results.push_back_deep(results.arena(), k);
		}

		return results;
	}

	struct BackupRangeTaskFunc : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<Key> beginKey() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Key> endKey() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<bool> addBackupRangeTasks() {
				return LiteralStringRef(__FUNCTION__);
			}
		} Params;

		std::string toString(Reference<Task> task) {
			return format("beginKey '%s' endKey '%s' addTasks %d", 
				Params.beginKey().get(task).printable().c_str(),
				Params.endKey().get(task).printable().c_str(),
				Params.addBackupRangeTasks().get(task)
			);
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

		// Finish (which flushes/syncs) the file, and then in a single transaction, make some range backup progress durable.  
		// This means:
		//  - increment the backup config's range bytes written
		//  - update the range file map
		//  - update the task begin key
		//  - save/extend the task with the new params
		// Returns whether or not the caller should continue executing the task.
		ACTOR static Future<bool> finishRangeFile(Reference<IBackupFile> file, Database cx, Reference<Task> task, Reference<TaskBucket> taskBucket, KeyRange range, Version version) {
			wait(file->finish());

			// Ignore empty ranges.
			if(range.empty())
				return false;

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			state BackupConfig backup(task);
			state bool usedFile = false;

			// Avoid unnecessary conflict by prevent taskbucket's automatic timeout extension
			// because the following transaction loop extends and updates the task.
			wait(task->extendMutex.take());
			state FlowLock::Releaser releaser(task->extendMutex, 1);

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					// Update the start key of the task so if this transaction completes but the task then fails
					// when it is restarted it will continue where this execution left off.
					Params.beginKey().set(task, range.end);

					// Save and extend the task with the new begin parameter
					state Version newTimeout = wait(taskBucket->extendTimeout(tr, task, true));

					// Update the range bytes written in the backup config
					backup.rangeBytesWritten().atomicOp(tr, file->size(), MutationRef::AddValue);

					// See if there is already a file for this key which has an earlier begin, update the map if not.
					Optional<BackupConfig::RangeSlice> s = wait(backup.snapshotRangeFileMap().get(tr, range.end));
					if(!s.present() || s.get().begin >= range.begin) {
						backup.snapshotRangeFileMap().set(tr, range.end, {range.begin, version, file->getFileName(), file->size()});
						usedFile = true;
					}

					wait(tr->commit());
					task->timeoutVersion = newTimeout;
					break;
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			return usedFile;
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, int priority, Key begin, Key end, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>(), Version scheduledVersion = invalidVersion) {
			Key key = wait(addBackupTask(BackupRangeTaskFunc::name,
										 BackupRangeTaskFunc::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask),
										 waitFor,
										 [=](Reference<Task> task) {
											 Params.beginKey().set(task, begin);
											 Params.endKey().set(task, end);
											 Params.addBackupRangeTasks().set(task, false);
											 if(scheduledVersion != invalidVersion)
												 ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
										 },
										 priority));
			return key;
		}

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

			wait(checkTaskVersion(cx, task, BackupRangeTaskFunc::name, BackupRangeTaskFunc::version));

			state Key beginKey = Params.beginKey().get(task);
			state Key endKey = Params.endKey().get(task);

			TraceEvent("FileBackupRangeStart")
				.suppressFor(60)
				.detail("BackupUID", BackupConfig(task).getUid())
				.detail("BeginKey", Params.beginKey().get(task).printable())
				.detail("EndKey", Params.endKey().get(task).printable())
				.detail("TaskKey", task->key.printable());

			// When a key range task saves the last chunk of progress and then the executor dies, when the task continues
			// its beginKey and endKey will be equal but there is no work to be done.
			if(beginKey == endKey)
				return Void();

			// Find out if there is a shard boundary in(beginKey, endKey)
			Standalone<VectorRef<KeyRef>> keys = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getBlockOfShards(tr, beginKey, endKey, 1); }));
			if (keys.size() > 0) {
				Params.addBackupRangeTasks().set(task, true);
				return Void();
			}

			// Read everything from beginKey to endKey, write it to an output file, run the output file processor, and
			// then set on_done. If we are still writing after X seconds, end the output file and insert a new backup_range
			// task for the remainder.
			state Reference<IBackupFile> outFile;
			state Version outVersion = invalidVersion;
			state Key lastKey;

			// retrieve kvData
			state PromiseStream<RangeResultWithVersion> results;

			state Future<Void> rc = readCommitted(cx, results, lock, KeyRangeRef(beginKey, endKey), true, true, true);
			state RangeFileWriter rangeFile;
			state BackupConfig backup(task);

			// Don't need to check keepRunning(task) here because we will do that while finishing each output file, but if bc
			// is false then clearly the backup is no longer in progress
			state Reference<IBackupContainer> bc = wait(backup.backupContainer().getD(cx));
			if(!bc) {
				return Void();
			}

			state bool done = false;
			state int64_t nrKeys = 0;

			loop{
				state RangeResultWithVersion values;
				try {
					RangeResultWithVersion _values = waitNext(results.getFuture());
					values = _values;
					lock->release(values.first.expectedSize());
				} catch(Error &e) {
					if(e.code() == error_code_end_of_stream)
						done = true;
					else
						throw;
				}

				// If we've seen a new read version OR hit the end of the stream, then if we were writing a file finish it.
				if (values.second != outVersion || done) {
					if (outFile){
						TEST(outVersion != invalidVersion); // Backup range task wrote multiple versions
						state Key nextKey = done ? endKey : keyAfter(lastKey);
						wait(rangeFile.writeKey(nextKey));

						bool usedFile = wait(finishRangeFile(outFile, cx, task, taskBucket, KeyRangeRef(beginKey, nextKey), outVersion));
						TraceEvent("FileBackupWroteRangeFile")
							.suppressFor(60)
							.detail("BackupUID", backup.getUid())
							.detail("Size", outFile->size())
							.detail("Keys", nrKeys)
							.detail("ReadVersion", outVersion)
							.detail("BeginKey", beginKey.printable())
							.detail("EndKey", nextKey.printable())
							.detail("AddedFileToMap", usedFile);

						nrKeys = 0;
						beginKey = nextKey;
					}

					if(done)
						return Void();

					// Start writing a new file 
					outVersion = values.second;
					// block size must be at least large enough for 3 max size keys and 2 max size values + overhead so 250k conservatively.
					state int blockSize = BUGGIFY ? g_random->randomInt(250e3, 4e6) : CLIENT_KNOBS->BACKUP_RANGEFILE_BLOCK_SIZE;
					Reference<IBackupFile> f = wait(bc->writeRangeFile(outVersion, blockSize));
					outFile = f;

					// Initialize range file writer and write begin key
					rangeFile = RangeFileWriter(outFile, blockSize);
					wait(rangeFile.writeKey(beginKey));
				}

				// write kvData to file, update lastKey and key count
				if(values.first.size() != 0) {
					state size_t i = 0;
					for (; i < values.first.size(); ++i) {
						wait(rangeFile.writeKV(values.first[i].key, values.first[i].value));
					}
					lastKey = values.first.back().key;
					nrKeys += values.first.size();
				}
			}
		}

		ACTOR static Future<Void> startBackupRangeInternal(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task, Reference<TaskFuture> onDone) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Key nextKey = Params.beginKey().get(task);
			state Key endKey = Params.endKey().get(task);

			state Standalone<VectorRef<KeyRef>> keys = wait(getBlockOfShards(tr, nextKey, endKey, CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT));

			std::vector<Future<Key>> addTaskVector;
			for (int idx = 0; idx < keys.size(); ++idx) {
				if (nextKey != keys[idx]) {
					addTaskVector.push_back(addTask(tr, taskBucket, task, task->getPriority(), nextKey, keys[idx], TaskCompletionKey::joinWith(onDone)));
					TraceEvent("FileBackupRangeSplit")
						.suppressFor(60)
						.detail("BackupUID", BackupConfig(task).getUid())
						.detail("BeginKey", Params.beginKey().get(task).printable())
						.detail("EndKey", Params.endKey().get(task).printable())
						.detail("SliceBeginKey", nextKey.printable())
						.detail("SliceEndKey", keys[idx].printable());
				}
				nextKey = keys[idx];
			}

			wait(waitForAll(addTaskVector));

			if (nextKey != endKey) {
				// Add task to cover nextKey to the end, using the priority of the current task
				Key _ = wait(addTask(tr, taskBucket, task, task->getPriority(), nextKey, endKey, TaskCompletionKey::joinWith(onDone), Reference<TaskFuture>(), task->getPriority()));
			}

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			if (Params.addBackupRangeTasks().get(task)) {
				wait(startBackupRangeInternal(tr, taskBucket, futureBucket, task, taskFuture));
			}
			else {
				wait(taskFuture->set(tr, taskBucket));
			}

			wait(taskBucket->finish(tr, task));

			TraceEvent("FileBackupRangeFinish")
				.suppressFor(60)
				.detail("BackupUID", BackupConfig(task).getUid())
				.detail("BeginKey", Params.beginKey().get(task).printable())
				.detail("EndKey", Params.endKey().get(task).printable())
				.detail("TaskKey", task->key.printable());

			return Void();
		}

	};
	StringRef BackupRangeTaskFunc::name = LiteralStringRef("file_backup_write_range_5.2");
	const uint32_t BackupRangeTaskFunc::version = 1;
	REGISTER_TASKFUNC(BackupRangeTaskFunc);

	struct BackupSnapshotDispatchTask : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			// Set by Execute, used by Finish
			static TaskParam<bool> snapshotFinished() {
				return LiteralStringRef(__FUNCTION__);
			}
			// Set by Execute, used by Finish
			static TaskParam<Version> nextDispatchVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
		} Params;

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, int priority, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>(), Version scheduledVersion = invalidVersion) {
			Key key = wait(addBackupTask(name,
										 version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask),
										 waitFor,
										 [=](Reference<Task> task) {
											 if(scheduledVersion != invalidVersion)
												 ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
										 },
										 priority));
			return key;
		}

		enum DispatchState { SKIP=0, DONE=1, NOT_DONE_MIN=2};

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));
			wait(checkTaskVersion(cx, task, name, version));

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			// The shard map will use 3 values classes.  Exactly SKIP, exactly DONE, then any number >= NOT_DONE_MIN which will mean not done.
			// This is to enable an efficient coalesce() call to squash adjacent ranges which are not yet finished to enable efficiently
			// finding random database shards which are not done.
			state int notDoneSequence = NOT_DONE_MIN;
			state KeyRangeMap<int> shardMap(notDoneSequence++, normalKeys.end);
			state Key beginKey = normalKeys.begin;

			// Read all shard boundaries and add them to the map
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					state Future<Standalone<VectorRef<KeyRef>>> shardBoundaries = getBlockOfShards(tr, beginKey, normalKeys.end, CLIENT_KNOBS->TOO_MANY);
					wait(success(shardBoundaries) && taskBucket->keepRunning(tr, task));

					if(shardBoundaries.get().size() == 0)
						break;

					for(auto &boundary : shardBoundaries.get()) {
						shardMap.rawInsert(boundary, notDoneSequence++);
					}

					beginKey = keyAfter(shardBoundaries.get().back());
					tr->reset();
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			// Read required stuff from backup config
			state BackupConfig config(task);
			state Version recentReadVersion;
			state Version snapshotBeginVersion;
			state Version snapshotTargetEndVersion;
			state int64_t snapshotIntervalSeconds;
			state Optional<Version> latestSnapshotEndVersion;
			state std::vector<KeyRange> backupRanges;
			state Optional<Key> snapshotBatchFutureKey;
			state Reference<TaskFuture> snapshotBatchFuture;
			state Optional<int64_t> snapshotBatchSize;

			tr->reset();
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					wait( store(config.snapshotBeginVersion().getOrThrow(tr), snapshotBeginVersion)
								&& store(config.snapshotTargetEndVersion().getOrThrow(tr), snapshotTargetEndVersion)
								&& store(config.backupRanges().getOrThrow(tr), backupRanges)
								&& store(config.snapshotIntervalSeconds().getOrThrow(tr), snapshotIntervalSeconds)
								// The next two parameters are optional
								&& store(config.snapshotBatchFuture().get(tr), snapshotBatchFutureKey)
								&& store(config.snapshotBatchSize().get(tr), snapshotBatchSize)
								&& store(config.latestSnapshotEndVersion().get(tr), latestSnapshotEndVersion)
								&& store(tr->getReadVersion(), recentReadVersion)
								&& taskBucket->keepRunning(tr, task));

					// If the snapshot batch future key does not exist, create it, set it, and commit
					// Also initialize the target snapshot end version if it is not yet set.
					if(!snapshotBatchFutureKey.present()) {
						snapshotBatchFuture = futureBucket->future(tr);
						config.snapshotBatchFuture().set(tr, snapshotBatchFuture->pack());
						snapshotBatchSize = 0;
						config.snapshotBatchSize().set(tr, snapshotBatchSize.get());

						// The dispatch of this batch can take multiple separate executions if the executor fails
						// so store a completion key for the dispatch finish() to set when dispatching the batch is done.
						state TaskCompletionKey dispatchCompletionKey = TaskCompletionKey::joinWith(snapshotBatchFuture);
						wait(map(dispatchCompletionKey.get(tr, taskBucket), [=](Key const &k) {
							config.snapshotBatchDispatchDoneKey().set(tr, k);
							return Void();
						}));

						wait(tr->commit());
					}
					else {
						ASSERT(snapshotBatchSize.present());
						// Batch future key exists in the config so create future from it
						snapshotBatchFuture = Reference<TaskFuture>(new TaskFuture(futureBucket, snapshotBatchFutureKey.get()));
					}

					break;
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			// Read all dispatched ranges
			state std::vector<std::pair<Key, bool>> dispatchBoundaries;
			tr->reset();
			beginKey = normalKeys.begin;
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					state Future<std::vector<std::pair<Key, bool>>> bounds = config.snapshotRangeDispatchMap().getRange(tr, beginKey, keyAfter(normalKeys.end), CLIENT_KNOBS->TOO_MANY);
					wait(success(bounds) && taskBucket->keepRunning(tr, task) && store(tr->getReadVersion(), recentReadVersion));

					if(bounds.get().empty())
						break;

					dispatchBoundaries.reserve(dispatchBoundaries.size() + bounds.get().size());
					dispatchBoundaries.insert(dispatchBoundaries.end(), bounds.get().begin(), bounds.get().end());

					beginKey = keyAfter(bounds.get().back().first);
					tr->reset();
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			// The next few sections involve combining the results above.  Yields are used after operations
			// that could have operated on many thousands of things and in loops which could have many
			// thousands of iterations.
			// Declare some common iterators which must be state vars and will be used multiple times.
			state int i;
			state RangeMap<Key, int, KeyRangeRef>::Iterator iShard;
			state RangeMap<Key, int, KeyRangeRef>::Iterator iShardEnd;

			// Set anything inside a dispatched range to DONE.
			// Also ensure that the boundary value are true, false, [true, false]...
			if(dispatchBoundaries.size() > 0) {
				state bool lastValue = false;
				state Key lastKey;
				for(i = 0; i < dispatchBoundaries.size(); ++i) {
					const std::pair<Key, bool> &boundary = dispatchBoundaries[i];

					// Values must alternate
					ASSERT(boundary.second == !lastValue);

					// If this was the end of a dispatched range
					if(!boundary.second) {
						// Ensure that the dispatched boundaries exist AND set all shard ranges in the dispatched range to DONE.
						RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges = shardMap.modify(KeyRangeRef(lastKey, boundary.first));
						iShard = shardRanges.begin();
						iShardEnd = shardRanges.end();
						for(; iShard != iShardEnd; ++iShard) {
							iShard->value() = DONE;
							wait(yield());
						}
					}
					lastValue = dispatchBoundaries[i].second;
					lastKey = dispatchBoundaries[i].first;

					wait(yield());
				}
				ASSERT(lastValue == false);
			}

			// Set anything outside the backup ranges to SKIP.  We can use insert() here instead of modify()
			// because it's OK to delete shard boundaries in the skipped ranges.
			if(backupRanges.size() > 0) {
				shardMap.insert(KeyRangeRef(normalKeys.begin, backupRanges.front().begin), SKIP);
				wait(yield());

				for(i = 0; i < backupRanges.size() - 1; ++i) {
					shardMap.insert(KeyRangeRef(backupRanges[i].end, backupRanges[i + 1].begin), SKIP);
					wait(yield());
				}

				shardMap.insert(KeyRangeRef(backupRanges.back().end, normalKeys.end), SKIP);
				wait(yield());
			}

			state int countShardsDone = 0;
			state int countShardsNotDone = 0;

			// Scan through the shard map, counting the DONE and NOT_DONE shards.
			RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges = shardMap.ranges();
			iShard = shardRanges.begin();
			iShardEnd = shardRanges.end();
			for(; iShard != iShardEnd; ++iShard) {
				if(iShard->value() == DONE) {
					++countShardsDone;
				}
				else if(iShard->value() >= NOT_DONE_MIN)
					++countShardsNotDone;

				wait(yield());
			}

			// Coalesce the shard map to make random selection below more efficient.
			shardMap.coalesce(normalKeys);
			wait(yield());

			// In this context "all" refers to all of the shards relevant for this particular backup
			state int countAllShards = countShardsDone + countShardsNotDone;

			if(countShardsNotDone == 0) {
				TraceEvent("FileBackupSnapshotDispatchFinished")
					.detail("BackupUID", config.getUid())
					.detail("AllShards", countAllShards)
					.detail("ShardsDone", countShardsDone)
					.detail("ShardsNotDone", countShardsNotDone)
					.detail("SnapshotBeginVersion", snapshotBeginVersion)
					.detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
					.detail("CurrentVersion", recentReadVersion)
					.detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);
				Params.snapshotFinished().set(task, true);
				return Void();
			}

			// Decide when the next snapshot dispatch should run.
			state Version nextDispatchVersion;

			// In simulation, use snapshot interval / 5 to ensure multiple dispatches run
			// Otherwise, use the knob for the number of seconds between snapshot dispatch tasks.
			if(g_network->isSimulated())
				nextDispatchVersion = recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND * (snapshotIntervalSeconds / 5.0);
			else
				nextDispatchVersion = recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND * CLIENT_KNOBS->BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC;

			// If nextDispatchVersion is greater than snapshotTargetEndVersion (which could be in the past) then just use
			// the greater of recentReadVersion or snapshotTargetEndVersion.  Any range tasks created in this dispatch will
			// be scheduled at a random time between recentReadVersion and nextDispatchVersion,
			// so nextDispatchVersion shouldn't be less than recentReadVersion.
			if(nextDispatchVersion > snapshotTargetEndVersion)
				nextDispatchVersion = std::max(recentReadVersion, snapshotTargetEndVersion);

			Params.nextDispatchVersion().set(task, nextDispatchVersion);

			// Calculate number of shards that should be done before the next interval end
			// timeElapsed is between 0 and 1 and represents what portion of the shards we should have completed by now
			double timeElapsed;
			if(snapshotTargetEndVersion > snapshotBeginVersion)
				timeElapsed = std::min(1.0, (double)(nextDispatchVersion - snapshotBeginVersion) / (snapshotTargetEndVersion - snapshotBeginVersion));
			else
				timeElapsed = 1.0;

			state int countExpectedShardsDone = countAllShards * timeElapsed;
			state int countShardsToDispatch = std::max<int>(0, countExpectedShardsDone - countShardsDone);

			TraceEvent("FileBackupSnapshotDispatchStats")
				.detail("BackupUID", config.getUid())
				.detail("AllShards", countAllShards)
				.detail("ShardsDone", countShardsDone)
				.detail("ShardsNotDone", countShardsNotDone)
				.detail("ExpectedShardsDone", countExpectedShardsDone)
				.detail("ShardsToDispatch", countShardsToDispatch)
				.detail("SnapshotBeginVersion", snapshotBeginVersion)
				.detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
				.detail("NextDispatchVersion", nextDispatchVersion)
				.detail("CurrentVersion", recentReadVersion)
				.detail("TimeElapsed", timeElapsed)
				.detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);

			// Dispatch random shards to catch up to the expected progress
			while(countShardsToDispatch > 0) {
				// First select ranges to add
				state std::vector<KeyRange> rangesToAdd;

				// Limit number of tasks added per transaction
				int taskBatchSize = BUGGIFY ? g_random->randomInt(1, countShardsToDispatch + 1) : CLIENT_KNOBS->BACKUP_DISPATCH_ADDTASK_SIZE;
				int added = 0;

				while(countShardsToDispatch > 0 && added < taskBatchSize && shardMap.size() > 0) {
					// Get a random range.
					auto it = shardMap.randomRange();
					// Find a NOT_DONE range and add it to rangesToAdd
					while(1) {
						if(it->value() >= NOT_DONE_MIN) {
							rangesToAdd.push_back(it->range());
							it->value() = DONE;
							shardMap.coalesce(Key(it->begin()));
							++added;
							++countShardsDone;
							--countShardsToDispatch;
							--countShardsNotDone;
							break;
						}
						if(it->end() == shardMap.mapEnd)
							break;
						++it;
					}
				}

				state int64_t oldBatchSize = snapshotBatchSize.get();
				state int64_t newBatchSize = oldBatchSize + rangesToAdd.size();

				// Now add the selected ranges in a single transaction.  
				tr->reset();
				loop {
					try {
						TraceEvent("FileBackupSnapshotDispatchAddingTasks")
							.suppressFor(2)
							.detail("TasksToAdd", rangesToAdd.size())
							.detail("NewBatchSize", newBatchSize);

						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						// For each range, make sure it isn't set in the dispatched range map.
						state std::vector<Future<Optional<bool>>> beginReads;
						state std::vector<Future<Optional<bool>>> endReads;

						for(auto &range : rangesToAdd) {
							beginReads.push_back(config.snapshotRangeDispatchMap().get(tr, range.begin));
							endReads.push_back(  config.snapshotRangeDispatchMap().get(tr, range.end));
						}

						wait(store(config.snapshotBatchSize().getOrThrow(tr), snapshotBatchSize.get())
								&& waitForAll(beginReads) && waitForAll(endReads) && taskBucket->keepRunning(tr, task));

						// Snapshot batch size should be either oldBatchSize or newBatchSize. If new, this transaction is already done.
						if(snapshotBatchSize.get() == newBatchSize) {
							break;
						}
						else {
							ASSERT(snapshotBatchSize.get() == oldBatchSize);
							config.snapshotBatchSize().set(tr, newBatchSize);
							snapshotBatchSize = newBatchSize;
						}

						state std::vector<Future<Void>> addTaskFutures;

						for(i = 0; i < beginReads.size(); ++i) {
							KeyRange &range = rangesToAdd[i];

							// This loop might have made changes to begin or end boundaries in a prior
							// iteration.  If so, the updated values exist in the RYW cache so re-read both entries.
							Optional<bool> beginValue = config.snapshotRangeDispatchMap().get(tr, range.begin).get();
							Optional<bool> endValue =   config.snapshotRangeDispatchMap().get(tr, range.end).get();

							ASSERT(!beginValue.present() || !endValue.present() || beginValue != endValue);

							// If begin is present, it must be a range end so value must be false
							// If end is present, it must be a range begin so value must be true
							if(    (!beginValue.present() || !beginValue.get())
								&& (!endValue.present()   ||  endValue.get())   )
							{
								if(beginValue.present()) {
									config.snapshotRangeDispatchMap().erase(tr, range.begin);
								}
								else {
									config.snapshotRangeDispatchMap().set(tr, range.begin, true);
								}
								if(endValue.present()) {
									config.snapshotRangeDispatchMap().erase(tr, range.end);
								}
								else {
									config.snapshotRangeDispatchMap().set(tr, range.end, false);
								}

								Version scheduledVersion = invalidVersion;
								// If the next dispatch version is in the future, choose a random version at which to start the new task.
								if(nextDispatchVersion > recentReadVersion)
									scheduledVersion = recentReadVersion + g_random->random01() * (nextDispatchVersion - recentReadVersion);

								// Range tasks during the initial snapshot should run at a higher priority
								int priority = latestSnapshotEndVersion.present() ? 0 : 1;
								addTaskFutures.push_back(success(BackupRangeTaskFunc::addTask(tr, taskBucket, task, priority, range.begin, range.end, TaskCompletionKey::joinWith(snapshotBatchFuture), Reference<TaskFuture>(), scheduledVersion)));

								TraceEvent("FileBackupSnapshotRangeDispatched")
									.suppressFor(2)
									.detail("BackupUID", config.getUid())
									.detail("CurrentVersion", recentReadVersion)
									.detail("ScheduledVersion", scheduledVersion)
									.detail("BeginKey", range.begin.printable())
									.detail("EndKey", range.end.printable());
							}
							else {
								// This shouldn't happen because if the transaction was already done or if another execution
								// of this task is making progress it should have been detected above.
								ASSERT(false);
							}
						}

						wait(waitForAll(addTaskFutures));
						wait(tr->commit());
						break;
					} catch(Error &e) {
						wait(tr->onError(e));
					}
				}
			}

			if(countShardsNotDone == 0) {
				TraceEvent("FileBackupSnapshotDispatchFinished")
					.detail("BackupUID", config.getUid())
					.detail("AllShards", countAllShards)
					.detail("ShardsDone", countShardsDone)
					.detail("ShardsNotDone", countShardsNotDone)
					.detail("SnapshotBeginVersion", snapshotBeginVersion)
					.detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
					.detail("CurrentVersion", recentReadVersion)
					.detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);
				Params.snapshotFinished().set(task, true);
			}

			return Void();
		}

		// This function is just a wrapper for BackupSnapshotManifest::addTask() which is defined below.
		// The BackupSnapshotDispatchTask and BackupSnapshotManifest tasks reference each other so in order to keep their execute and finish phases
		// defined together inside their class definitions this wrapper is declared here but defined after BackupSnapshotManifest is defined.
		static Future<Key> addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>());

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state BackupConfig config(task);

			// Get the batch future and dispatch done keys, then clear them.
			state Key snapshotBatchFutureKey;
			state Key snapshotBatchDispatchDoneKey;

			wait( store(config.snapshotBatchFuture().getOrThrow(tr), snapshotBatchFutureKey)
						&& store(config.snapshotBatchDispatchDoneKey().getOrThrow(tr), snapshotBatchDispatchDoneKey));

			state Reference<TaskFuture> snapshotBatchFuture = futureBucket->unpack(snapshotBatchFutureKey);
			state Reference<TaskFuture> snapshotBatchDispatchDoneFuture = futureBucket->unpack(snapshotBatchDispatchDoneKey);
			config.snapshotBatchFuture().clear(tr);
			config.snapshotBatchDispatchDoneKey().clear(tr);
			config.snapshotBatchSize().clear(tr);

			state Reference<TaskFuture> snapshotFinishedFuture = task->getDoneFuture(futureBucket);

			// If the snapshot is finished, the next task is to write a snapshot manifest, otherwise it's another snapshot dispatch task.
			// In either case, the task should wait for snapshotBatchFuture.
			// The snapshot done key, passed to the current task, is also passed on.
			if(Params.snapshotFinished().getOrDefault(task, false)) {
				wait(success(addSnapshotManifestTask(tr, taskBucket, task, TaskCompletionKey::signal(snapshotFinishedFuture), snapshotBatchFuture)));
			}
			else {
				wait(success(addTask(tr, taskBucket, task, 1, TaskCompletionKey::signal(snapshotFinishedFuture), snapshotBatchFuture, Params.nextDispatchVersion().get(task))));
			}

			// This snapshot batch is finished, so set the batch done future.
			wait(snapshotBatchDispatchDoneFuture->set(tr, taskBucket));

			wait(taskBucket->finish(tr, task));

			return Void();
		}

	};
	StringRef BackupSnapshotDispatchTask::name = LiteralStringRef("file_backup_dispatch_ranges_5.2");
	const uint32_t BackupSnapshotDispatchTask::version = 1;
	REGISTER_TASKFUNC(BackupSnapshotDispatchTask);

	struct BackupLogRangeTaskFunc : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<bool> addBackupLogRangeTasks() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<int64_t> fileSize() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Version> beginVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Version> endVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
		} Params;

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

			wait(checkTaskVersion(cx, task, BackupLogRangeTaskFunc::name, BackupLogRangeTaskFunc::version));

			state Version beginVersion = Params.beginVersion().get(task);
			state Version endVersion = Params.endVersion().get(task);

			state BackupConfig config(task);
			state Reference<IBackupContainer> bc;

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop{
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				// Wait for the read version to pass endVersion
				try {
					wait(taskBucket->keepRunning(tr, task));

					if(!bc) {
						// Backup container must be present if we're still here
						Reference<IBackupContainer> _bc = wait(config.backupContainer().getOrThrow(tr));
						bc = _bc;
					}

					Version currentVersion = tr->getReadVersion().get();
					if(endVersion < currentVersion)
						break;

					wait(delay(std::max(CLIENT_KNOBS->BACKUP_RANGE_MINWAIT, (double) (endVersion-currentVersion)/CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
					tr->reset();
				}
				catch (Error &e) {
					wait(tr->onError(e));
				}
			}

			Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
			state Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion, endVersion, destUidValue);
			if (ranges.size() > CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
				Params.addBackupLogRangeTasks().set(task, true);
				return Void();
			}

			// Block size must be at least large enough for 1 max size key, 1 max size value, and overhead, so conservatively 125k.
			state int blockSize = BUGGIFY ? g_random->randomInt(125e3, 4e6) : CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
			state Reference<IBackupFile> outFile = wait(bc->writeLogFile(beginVersion, endVersion, blockSize));
			state LogFileWriter logFile(outFile, blockSize);
			state size_t idx;

			state PromiseStream<RangeResultWithVersion> results;
			state std::vector<Future<Void>> rc;

			for (auto &range : ranges) {
				rc.push_back(readCommitted(cx, results, lock, range, false, true, true));
			}
			
			state Future<Void> sendEOS = map(errorOr(waitForAll(rc)), [=](ErrorOr<Void> const &result) {
				if(result.isError())
					results.sendError(result.getError());
				else
					results.sendError(end_of_stream());
				return Void();
			});

			state Version lastVersion;
			try {
				loop {
					state RangeResultWithVersion r = waitNext(results.getFuture());
					lock->release(r.first.expectedSize());

					state int i = 0;
					for (; i < r.first.size(); ++i) {
						// Remove the backupLogPrefix + UID bytes from the key
						wait(logFile.writeKV(r.first[i].key.substr(backupLogPrefixBytes + 16), r.first[i].value));
						lastVersion = r.second;
					}
				}
			} catch (Error &e) {
				if(e.code() == error_code_actor_cancelled)
					throw;

				if (e.code() != error_code_end_of_stream) {
					state Error err = e;
					wait(config.logError(cx, err, format("Failed to write to file `%s'", outFile->getFileName().c_str())));
					throw err;
				}
			}

			// Make sure this task is still alive, if it's not then the data read above could be incomplete.
			wait(taskBucket->keepRunning(cx, task));

			wait(outFile->finish());

			TraceEvent("FileBackupWroteLogFile")
				.suppressFor(60)
				.detail("BackupUID", config.getUid())
				.detail("Size", outFile->size())
				.detail("BeginVersion", beginVersion)
				.detail("EndVersion", endVersion)
				.detail("LastReadVersion", latestVersion);

			Params.fileSize().set(task, outFile->size());

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, int priority, Version beginVersion, Version endVersion, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key key = wait(addBackupTask(BackupLogRangeTaskFunc::name,
										 BackupLogRangeTaskFunc::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask),
										 waitFor,
										 [=](Reference<Task> task) {
											 Params.beginVersion().set(task, beginVersion);
											 Params.endVersion().set(task, endVersion);
											 Params.addBackupLogRangeTasks().set(task, false);
										 },
										 priority));
			return key;
		}

		ACTOR static Future<Void> startBackupLogRangeInternal(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task, Reference<TaskFuture> taskFuture, Version beginVersion, Version endVersion) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			std::vector<Future<Key>> addTaskVector;
			int tasks = 0;
			for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE; vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE; vblock += CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
				Version bv = std::max(beginVersion, vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

				if( tasks >= CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT ) {
					addTaskVector.push_back(addTask(tr, taskBucket, task, task->getPriority(), bv, endVersion, TaskCompletionKey::joinWith(taskFuture)));
					break;
				}

				Version ev = std::min(endVersion, (vblock + CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
				addTaskVector.push_back(addTask(tr, taskBucket, task, task->getPriority(), bv, ev, TaskCompletionKey::joinWith(taskFuture)));
				tasks++;
			}

			wait(waitForAll(addTaskVector));

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Version beginVersion = Params.beginVersion().get(task);
			state Version endVersion = Params.endVersion().get(task);
			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			state BackupConfig config(task);

			if(Params.fileSize().exists(task)) {
				config.logBytesWritten().atomicOp(tr, Params.fileSize().get(task), MutationRef::AddValue);
			}

			if (Params.addBackupLogRangeTasks().get(task)) {
				wait(startBackupLogRangeInternal(tr, taskBucket, futureBucket, task, taskFuture, beginVersion, endVersion));
				endVersion = beginVersion;
			} else {
				wait(taskFuture->set(tr, taskBucket));
			}

			wait(taskBucket->finish(tr, task));
			return Void();
		}
	};

	StringRef BackupLogRangeTaskFunc::name = LiteralStringRef("file_backup_write_logs_5.2");
	const uint32_t BackupLogRangeTaskFunc::version = 1;
	REGISTER_TASKFUNC(BackupLogRangeTaskFunc);

	struct EraseLogRangeTaskFunc : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		static struct {
			static TaskParam<Version> beginVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Version> endVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Key> destUidValue() {
				return LiteralStringRef(__FUNCTION__);
			}
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));
			wait(checkTaskVersion(cx, task, EraseLogRangeTaskFunc::name, EraseLogRangeTaskFunc::version));

			state Version endVersion = Params.endVersion().get(task);
			state Key destUidValue = Params.destUidValue().get(task);

			state BackupConfig config(task);
			state Key logUidValue = config.getUidAsKey();

			wait(eraseLogData(cx, logUidValue, destUidValue, endVersion != 0 ? Optional<Version>(endVersion) : Optional<Version>()));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, UID logUid, TaskCompletionKey completionKey, Key destUidValue, Version endVersion = 0, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key key = wait(addBackupTask(EraseLogRangeTaskFunc::name,
										 EraseLogRangeTaskFunc::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(logUid),
										 waitFor,
										 [=](Reference<Task> task) {
											 Params.beginVersion().set(task, 1); //FIXME: remove in 6.X, only needed for 5.2 backward compatibility
											 Params.endVersion().set(task, endVersion);
											 Params.destUidValue().set(task, destUidValue);
										 },
										 0, false));

			return key;
		}


		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));

			return Void();
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef EraseLogRangeTaskFunc::name = LiteralStringRef("file_backup_erase_logs_5.2");
	const uint32_t EraseLogRangeTaskFunc::version = 1;
	REGISTER_TASKFUNC(EraseLogRangeTaskFunc);



	struct BackupLogsDispatchTask : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<Version> prevBeginVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
			static TaskParam<Version> beginVersion() {
				return LiteralStringRef(__FUNCTION__);
			}
		} Params;

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			wait(checkTaskVersion(tr->getDatabase(), task, BackupLogsDispatchTask::name, BackupLogsDispatchTask::version));

			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state Reference<TaskFuture> onDone = task->getDoneFuture(futureBucket);
			state Version prevBeginVersion = Params.prevBeginVersion().get(task);
			state Version beginVersion = Params.beginVersion().get(task);
			state BackupConfig config(task);
			config.latestLogEndVersion().set(tr, beginVersion);

			state bool stopWhenDone;
			state Optional<Version> restorableVersion;
			state EBackupState backupState;
			state Optional<std::string> tag;
			state Optional<Version> latestSnapshotEndVersion;

			wait(store(config.stopWhenDone().getOrThrow(tr), stopWhenDone) 
						&& store(config.getLatestRestorableVersion(tr), restorableVersion)
						&& store(config.stateEnum().getOrThrow(tr), backupState)
						&& store(config.tag().get(tr), tag)
						&& store(config.latestSnapshotEndVersion().get(tr), latestSnapshotEndVersion));

			// If restorable, update the last restorable version for this tag
			if(restorableVersion.present() && tag.present()) {
				FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
			}

			// If the backup is restorable but the state is not differential then set state to differential
			if(restorableVersion.present() && backupState != BackupAgentBase::STATE_DIFFERENTIAL)
				config.stateEnum().set(tr, BackupAgentBase::STATE_DIFFERENTIAL);

			// If stopWhenDone is set and there is a restorable version, set the done future and do not create further tasks.
			if(stopWhenDone && restorableVersion.present()) {
				wait(onDone->set(tr, taskBucket) && taskBucket->finish(tr, task));

				TraceEvent("FileBackupLogsDispatchDone")
					.detail("BackupUID", config.getUid())
					.detail("BeginVersion", beginVersion)
					.detail("RestorableVersion", restorableVersion.orDefault(-1));

				return Void();
			}

			state Version endVersion = std::max<Version>( tr->getReadVersion().get() + 1, beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES-1)*CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE );

			TraceEvent("FileBackupLogDispatch")
				.suppressFor(60)
				.detail("BeginVersion", beginVersion)
				.detail("EndVersion", endVersion)
				.detail("RestorableVersion", restorableVersion.orDefault(-1));

			state Reference<TaskFuture> logDispatchBatchFuture = futureBucket->future(tr);

			// If a snapshot has ended for this backup then mutations are higher priority to reduce backup lag
			state int priority = latestSnapshotEndVersion.present() ? 1 : 0;

			// Add the initial log range task to read/copy the mutations and the next logs dispatch task which will run after this batch is done
			Key _ = wait(BackupLogRangeTaskFunc::addTask(tr, taskBucket, task, priority, beginVersion, endVersion, TaskCompletionKey::joinWith(logDispatchBatchFuture)));
			Key _ = wait(BackupLogsDispatchTask::addTask(tr, taskBucket, task, priority, beginVersion, endVersion, TaskCompletionKey::signal(onDone), logDispatchBatchFuture));

			// Do not erase at the first time
			if (prevBeginVersion > 0) {
				state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
				Key _ = wait(EraseLogRangeTaskFunc::addTask(tr, taskBucket, config.getUid(), TaskCompletionKey::joinWith(logDispatchBatchFuture), destUidValue, beginVersion));
			}

			wait(taskBucket->finish(tr, task));

			TraceEvent("FileBackupLogsDispatchContinuing")
				.suppressFor(60)
				.detail("BackupUID", config.getUid())
				.detail("BeginVersion", beginVersion)
				.detail("EndVersion", endVersion);

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, int priority, Version prevBeginVersion, Version beginVersion, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key key = wait(addBackupTask(BackupLogsDispatchTask::name,
										 BackupLogsDispatchTask::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask),
										 waitFor,
										 [=](Reference<Task> task) {
											 Params.prevBeginVersion().set(task, prevBeginVersion);
											 Params.beginVersion().set(task, beginVersion);
										 },
										 priority));
			return key;
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef BackupLogsDispatchTask::name = LiteralStringRef("file_backup_dispatch_logs_5.2");
	const uint32_t BackupLogsDispatchTask::version = 1;
	REGISTER_TASKFUNC(BackupLogsDispatchTask);

	struct FileBackupFinishedTask : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		StringRef getName() const { return name; };

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			wait(checkTaskVersion(tr->getDatabase(), task, FileBackupFinishedTask::name, FileBackupFinishedTask::version));

			state BackupConfig backup(task);
			state UID uid = backup.getUid();

			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
			state Key destUidValue = wait(backup.destUidValue().getOrThrow(tr));
			Key _ = wait(EraseLogRangeTaskFunc::addTask(tr, taskBucket, backup.getUid(), TaskCompletionKey::noSignal(), destUidValue));
			
			backup.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

			wait(taskBucket->finish(tr, task));

			TraceEvent("FileBackupFinished").detail("BackupUID", uid);

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key key = wait(addBackupTask(FileBackupFinishedTask::name,
										 FileBackupFinishedTask::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask), waitFor));
			return key;
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef FileBackupFinishedTask::name = LiteralStringRef("file_backup_finished_5.2");
	const uint32_t FileBackupFinishedTask::version = 1;
	REGISTER_TASKFUNC(FileBackupFinishedTask);

	struct BackupSnapshotManifest : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		static struct {
			static TaskParam<Version> endVersion() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state BackupConfig config(task);
			state Reference<IBackupContainer> bc;

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			// Read the entire range file map into memory, then walk it backwards from its last entry to produce a list of non overlapping key range files
			state std::map<Key, BackupConfig::RangeSlice> localmap;
			state Key startKey;
			state int batchSize = BUGGIFY ? 1 : 1000000;

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					wait(taskBucket->keepRunning(tr, task));

					if(!bc) {
						// Backup container must be present if we're still here
						wait(store(config.backupContainer().getOrThrow(tr), bc));
					}

					BackupConfig::RangeFileMapT::PairsType rangeresults = wait(config.snapshotRangeFileMap().getRange(tr, startKey, {}, batchSize));

					for(auto &p : rangeresults) {
						localmap.insert(p);
					}

					if(rangeresults.size() < batchSize)
						break;

					startKey = keyAfter(rangeresults.back().first);
					tr->reset();
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			std::vector<std::string> files;
			state Version maxVer = 0;
			state Version minVer = std::numeric_limits<Version>::max();
			state int64_t totalBytes = 0;

			if(!localmap.empty()) {
				// Get iterator that points to greatest key, start there.
				auto ri = localmap.rbegin();
				auto i = (++ri).base();

				while(1) {
					const BackupConfig::RangeSlice &r = i->second;

					// Add file to final file list
					files.push_back(r.fileName);

					// Update version range seen
					if(r.version < minVer)
						minVer = r.version;
					if(r.version > maxVer)
						maxVer = r.version;

					// Update total bytes counted.
					totalBytes += r.fileSize;

					// Jump to file that either ends where this file begins or has the greatest end that is less than
					// the begin of this file.  In other words find the map key that is <= begin of this file.  To do this
					// find the first end strictly greater than begin and then back up one.
					i = localmap.upper_bound(i->second.begin);
					// If we get begin then we're done, there are no more ranges that end at or before the last file's begin
					if(i == localmap.begin())
						break;
					--i;
				}
			}

			Params.endVersion().set(task, maxVer);
			wait(bc->writeKeyspaceSnapshotFile(files, totalBytes));

			TraceEvent(SevInfo, "FileBackupWroteSnapshotManifest")
				.detail("BackupUID", config.getUid())
				.detail("BeginVersion", minVer)
				.detail("EndVersion", maxVer)
				.detail("TotalBytes", totalBytes);

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			wait(checkTaskVersion(tr->getDatabase(), task, BackupSnapshotManifest::name, BackupSnapshotManifest::version));

			state BackupConfig config(task);

			// Set the latest snapshot end version, which was set during the execute phase
			config.latestSnapshotEndVersion().set(tr, Params.endVersion().get(task));

			state bool stopWhenDone;
			state EBackupState backupState;
			state Optional<Version> restorableVersion;
			state Optional<Version> firstSnapshotEndVersion;
			state Optional<std::string> tag;

			wait(store(config.stopWhenDone().getOrThrow(tr), stopWhenDone) 
						&& store(config.stateEnum().getOrThrow(tr), backupState)
						&& store(config.getLatestRestorableVersion(tr), restorableVersion)
						&& store(config.firstSnapshotEndVersion().get(tr), firstSnapshotEndVersion)
						&& store(config.tag().get(tr), tag));

			// If restorable, update the last restorable version for this tag
			if(restorableVersion.present() && tag.present()) {
				FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
			}

			if(!firstSnapshotEndVersion.present()) {
				config.firstSnapshotEndVersion().set(tr, Params.endVersion().get(task));
			}

			// If the backup is restorable and the state isn't differential the set state to differential
			if(restorableVersion.present() && backupState != BackupAgentBase::STATE_DIFFERENTIAL)
				config.stateEnum().set(tr, BackupAgentBase::STATE_DIFFERENTIAL);

			// Unless we are to stop, start the next snapshot using the default interval
			Reference<TaskFuture> snapshotDoneFuture = task->getDoneFuture(futureBucket);
			if(!stopWhenDone) {
				wait(config.initNewSnapshot(tr) && success(BackupSnapshotDispatchTask::addTask(tr, taskBucket, task, 1, TaskCompletionKey::signal(snapshotDoneFuture))));
			} else {
				// Set the done future as the snapshot is now complete.
				wait(snapshotDoneFuture->set(tr, taskBucket));
			}

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key key = wait(addBackupTask(BackupSnapshotManifest::name,
										 BackupSnapshotManifest::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(parentTask), waitFor, NOP_SETUP_TASK_FN, 1));
			return key;
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef BackupSnapshotManifest::name = LiteralStringRef("file_backup_write_snapshot_manifest_5.2");
	const uint32_t BackupSnapshotManifest::version = 1;
	REGISTER_TASKFUNC(BackupSnapshotManifest);

	Future<Key> BackupSnapshotDispatchTask::addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor) {
		return BackupSnapshotManifest::addTask(tr, taskBucket, parentTask, completionKey, waitFor);
	}

	struct StartFullBackupTaskFunc : BackupTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			wait(checkTaskVersion(cx, task, StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version));

			loop{
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					Version startVersion = wait(tr->getReadVersion());

					Params.beginVersion().set(task, startVersion);
					break;
				}
				catch (Error &e) {
					wait(tr->onError(e));
				}
			}

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state BackupConfig config(task);
			state Version beginVersion = Params.beginVersion().get(task);

			state Future<std::vector<KeyRange>> backupRangesFuture = config.backupRanges().getOrThrow(tr);
			state Future<Key> destUidValueFuture = config.destUidValue().getOrThrow(tr);
			wait(success(backupRangesFuture) && success(destUidValueFuture));
			std::vector<KeyRange> backupRanges = backupRangesFuture.get();
			Key destUidValue = destUidValueFuture.get();

			// Start logging the mutations for the specified ranges of the tag
			for (auto &backupRange : backupRanges) {
				config.startMutationLogs(tr, backupRange, destUidValue);
			}

			config.stateEnum().set(tr, EBackupState::STATE_BACKUP);

			state Reference<TaskFuture>	backupFinished = futureBucket->future(tr);

			// Initialize the initial snapshot and create tasks to continually write logs and snapshots
			// The initial snapshot has a desired duration of 0, meaning go as fast as possible.
			wait(config.initNewSnapshot(tr, 0));

			// Using priority 1 for both of these to at least start both tasks soon
			Key _ = wait(BackupSnapshotDispatchTask::addTask(tr, taskBucket, task, 1, TaskCompletionKey::joinWith(backupFinished)));
			Key _ = wait(BackupLogsDispatchTask::addTask(tr, taskBucket, task, 1, 0, beginVersion, TaskCompletionKey::joinWith(backupFinished)));

			// If a clean stop is requested, the log and snapshot tasks will quit after the backup is restorable, then the following
			// task will clean up and set the completed state.
			Key _ = wait(FileBackupFinishedTask::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), backupFinished));

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, UID uid, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>())
		{
			Key key = wait(addBackupTask(StartFullBackupTaskFunc::name,
										 StartFullBackupTaskFunc::version,
										 tr, taskBucket, completionKey,
										 BackupConfig(uid), waitFor));
			return key;
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef StartFullBackupTaskFunc::name = LiteralStringRef("file_backup_start_5.2");
	const uint32_t StartFullBackupTaskFunc::version = 1;
	REGISTER_TASKFUNC(StartFullBackupTaskFunc);

	struct RestoreCompleteTaskFunc : RestoreTaskFuncBase {
		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			wait(checkTaskVersion(tr->getDatabase(), task, name, version));

			state RestoreConfig restore(task);
			restore.stateEnum().set(tr, ERestoreState::COMPLETED);
			// Clear the file map now since it could be huge.
			restore.fileSet().clear(tr);

			// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any restore operation
			// the ranges to restore must be within the backed up ranges, otherwise from the restore perspective it will appear that some
			// key ranges were missing and so the backup set is incomplete and the restore has failed.
			// This validation cannot be done currently because Restore only supports a single restore range but backups can have many ranges.

			// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
			restore.clearApplyMutationsKeys(tr);

			wait(taskBucket->finish(tr, task));
			wait(unlockDatabase(tr, restore.getUid()));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreCompleteTaskFunc::name, RestoreCompleteTaskFunc::version, doneKey));

			// Get restore config from parent task and bind it to new task
			wait(RestoreConfig(parentTask).toTask(tr, task));

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

	};
	StringRef RestoreCompleteTaskFunc::name = LiteralStringRef("restore_complete");
	const uint32_t RestoreCompleteTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreCompleteTaskFunc);

	struct RestoreFileTaskFuncBase : RestoreTaskFuncBase {
		struct InputParams {
			static TaskParam<RestoreFile> inputFile() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> readOffset() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> readLen() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		std::string toString(Reference<Task> task) {
			return format("fileName '%s' readLen %lld readOffset %lld",
				Params.inputFile().get(task).fileName.c_str(),
				Params.readLen().get(task),
				Params.readOffset().get(task));
		}
	};

	struct RestoreRangeTaskFunc : RestoreFileTaskFuncBase {
		static struct : InputParams {
			// The range of data that the (possibly empty) data represented, which is set if it intersects the target restore range
			static TaskParam<KeyRange> originalFileRange() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		std::string toString(Reference<Task> task) {
			return RestoreFileTaskFuncBase::toString(task) + format(" originalFileRange '%s'", printable(Params.originalFileRange().get(task)).c_str());
		}

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);

			state RestoreFile rangeFile = Params.inputFile().get(task);
			state int64_t readOffset = Params.readOffset().get(task);
			state int64_t readLen = Params.readLen().get(task);

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

					wait(taskBucket->keepRunning(tr, task));

					wait(success(bc) && success(restoreRange) && success(addPrefix) && success(removePrefix) && checkTaskVersion(tr->getDatabase(), task, name, version));
					break;

				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			state Reference<IAsyncFile> inFile = wait(bc.get()->readFile(rangeFile.fileName));
			state Standalone<VectorRef<KeyValueRef>> blockData = wait(decodeRangeFileBlock(inFile, readOffset, readLen));

			// First and last key are the range for this file
			state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

			// If fileRange doesn't intersect restore range then we're done.
			if(!fileRange.intersects(restoreRange.get()))
				return Void();

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
			Params.originalFileRange().set(task, originalFileRange);

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

					wait(taskBucket->keepRunning(tr, task));

					wait( checkLock );

					wait(tr->commit());

					TraceEvent("FileRestoreCommittedRange")
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

					if(start == end)
						return Void();
					tr->reset();
				} catch(Error &e) {
					if(e.code() == error_code_transaction_too_large)
						dataSizeLimit /= 2;
					else
						wait(tr->onError(e));
				}
			}
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);
			restore.fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

			// Update the KV range map if originalFileRange is set
			Future<Void> updateMap = Void();
			if(Params.originalFileRange().exists(task)) {
				Value versionEncoded = BinaryWriter::toValue(Params.inputFile().get(task).version, Unversioned());
				updateMap = krmSetRange(tr, restore.applyMutationsMapPrefix(), Params.originalFileRange().get(task), versionEncoded);
			}

			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			wait(taskFuture->set(tr, taskBucket) &&
					        taskBucket->finish(tr, task) && updateMap);

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, RestoreFile rf, int64_t offset, int64_t len, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreRangeTaskFunc::name, RestoreRangeTaskFunc::version, doneKey));

			// Create a restore config from the current task and bind it to the new task.
			wait(RestoreConfig(parentTask).toTask(tr, task));

			Params.inputFile().set(task, rf);
			Params.readOffset().set(task, offset);
			Params.readLen().set(task, len);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreRangeTaskFunc::name = LiteralStringRef("restore_range_data");
	const uint32_t RestoreRangeTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreRangeTaskFunc);

	struct RestoreLogDataTaskFunc : RestoreFileTaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		static struct : InputParams {
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);

			state RestoreFile logFile = Params.inputFile().get(task);
			state int64_t readOffset = Params.readOffset().get(task);
			state int64_t readLen = Params.readLen().get(task);

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

					wait(checkTaskVersion(tr->getDatabase(), task, name, version));
					wait(taskBucket->keepRunning(tr, task));

					break;
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			state Key mutationLogPrefix = restore.mutationLogPrefix();
			state Reference<IAsyncFile> inFile = wait(bc->readFile(logFile.fileName));
			state Standalone<VectorRef<KeyValueRef>> data = wait(decodeLogFileBlock(inFile, readOffset, readLen));

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

					wait(taskBucket->keepRunning(tr, task));
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

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			RestoreConfig(task).fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			// TODO:  Check to see if there is a leak in the FutureBucket since an invalid task (validation key fails) will never set its taskFuture.
			wait(taskFuture->set(tr, taskBucket) &&
					        taskBucket->finish(tr, task));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, RestoreFile lf, int64_t offset, int64_t len, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreLogDataTaskFunc::name, RestoreLogDataTaskFunc::version, doneKey));

			// Create a restore config from the current task and bind it to the new task.
			wait(RestoreConfig(parentTask).toTask(tr, task));
			Params.inputFile().set(task, lf);
			Params.readOffset().set(task, offset);
			Params.readLen().set(task, len);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreLogDataTaskFunc::name = LiteralStringRef("restore_log_data");
	const uint32_t RestoreLogDataTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreLogDataTaskFunc);

	struct RestoreDispatchTaskFunc : RestoreTaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		static struct {
			static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<std::string> beginFile() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> beginBlock() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> batchSize() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> remainingInBatch() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		//MX: This is the function that see the restore task is done. it traces "restore_complete". This part of code should go into restore agents
		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);

			state Version beginVersion = Params.beginVersion().get(task);
			state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			state int64_t remainingInBatch = Params.remainingInBatch().get(task);
			state bool addingToExistingBatch = remainingInBatch > 0;
			state Version restoreVersion;

			wait(store(restore.restoreVersion().getOrThrow(tr), restoreVersion)
						&& checkTaskVersion(tr->getDatabase(), task, name, version));

			// If not adding to an existing batch then update the apply mutations end version so the mutations from the
			// previous batch can be applied.  Only do this once beginVersion is > 0 (it will be 0 for the initial dispatch).
			//MX: Must setApplyEndVersion to trigger master proxy to apply mutation to DB.
			if(!addingToExistingBatch && beginVersion > 0) {
				restore.setApplyEndVersion(tr, std::min(beginVersion, restoreVersion + 1));
			}

			// The applyLag must be retrieved AFTER potentially updating the apply end version.
			state int64_t applyLag = wait(restore.getApplyVersionLag(tr));
			state int64_t batchSize = Params.batchSize().get(task);

			// If starting a new batch and the apply lag is too large then re-queue and wait
			if(!addingToExistingBatch && applyLag > (BUGGIFY ? 1 : CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 300)) {
				// Wait a small amount of time and then re-add this same task.
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, "", 0, batchSize, remainingInBatch));

				TraceEvent("FileRestoreDispatch")
					.detail("RestoreUID", restore.getUid())
					.detail("BeginVersion", beginVersion)
					.detail("ApplyLag", applyLag)
					.detail("BatchSize", batchSize)
					.detail("Decision", "too_far_behind")
					.detail("TaskInstance", (uint64_t)this);

				wait(taskBucket->finish(tr, task));
				return Void();
			}

			state std::string beginFile = Params.beginFile().getOrDefault(task);
			// Get a batch of files.  We're targeting batchSize blocks being dispatched so query for batchSize files (each of which is 0 or more blocks).
			state int taskBatchSize = BUGGIFY ? 1 : CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE;
			state RestoreConfig::FileSetT::Values files = wait(restore.fileSet().getRange(tr, {beginVersion, beginFile}, {}, taskBatchSize));

			if ( files.size() )
				TraceEvent("FileBackupAgentFinishMX").detail("MX", 1).detail("RestoureConfigFiles", files.size());
			for(; i < files.size(); ++i) {
				RestoreConfig::RestoreFile &f = files[i];
				TraceEvent("RestoureConfigFiles").detail("Index", i).detail("FileInfo", f.toString());
			}

			// allPartsDone will be set once all block tasks in the current batch are finished.
			state Reference<TaskFuture> allPartsDone;

			// If adding to existing batch then join the new block tasks to the existing batch future
			if(addingToExistingBatch) {
				Key fKey = wait(restore.batchFuture().getD(tr));
				allPartsDone = Reference<TaskFuture>(new TaskFuture(futureBucket, fKey));
			}
			else {
				// Otherwise create a new future for the new batch
				allPartsDone = futureBucket->future(tr);
				restore.batchFuture().set(tr, allPartsDone->pack());
				// Set batch quota remaining to batch size
				remainingInBatch = batchSize;
			}

			// If there were no files to load then this batch is done and restore is almost done.
			if(files.size() == 0) {
				// If adding to existing batch then blocks could be in progress so create a new Dispatch task that waits for them to finish
				if(addingToExistingBatch) {
					// Setting next begin to restoreVersion + 1 so that any files in the file map at the restore version won't be dispatched again.
					Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion + 1, "", 0, batchSize, 0, TaskCompletionKey::noSignal(), allPartsDone));

					TraceEvent("FileRestoreDispatch")
						.detail("RestoreUID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("BeginFile", printable(Params.beginFile().get(task)))
						.detail("BeginBlock", Params.beginBlock().get(task))
						.detail("RestoreVersion", restoreVersion)
						.detail("ApplyLag", applyLag)
						.detail("Decision", "end_of_final_batch")
						.detail("TaskInstance", (uint64_t)this);
				}
				else if(beginVersion < restoreVersion) {
					// If beginVersion is less than restoreVersion then do one more dispatch task to get there
					Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion, "", 0, batchSize));

					TraceEvent("FileRestoreDispatch")
						.detail("RestoreUID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("BeginFile", printable(Params.beginFile().get(task)))
						.detail("BeginBlock", Params.beginBlock().get(task))
						.detail("RestoreVersion", restoreVersion)
						.detail("ApplyLag", applyLag)
						.detail("Decision", "apply_to_restore_version")
						.detail("TaskInstance", (uint64_t)this);
				}
				else if(applyLag == 0) {
					// If apply lag is 0 then we are done so create the completion task
					Key _ = wait(RestoreCompleteTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal()));

					TraceEvent("FileRestoreDispatch")
						.detail("RestoreUID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("BeginFile", printable(Params.beginFile().get(task)))
						.detail("BeginBlock", Params.beginBlock().get(task))
						.detail("ApplyLag", applyLag)
						.detail("Decision", "restore_complete")
						.detail("TaskInstance", (uint64_t)this);
				} else {
					// Applying of mutations is not yet finished so wait a small amount of time and then re-add this same task.
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, "", 0, batchSize));

					TraceEvent("FileRestoreDispatch")
						.detail("RestoreUID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("ApplyLag", applyLag)
						.detail("Decision", "apply_still_behind")
						.detail("TaskInstance", (uint64_t)this);
				}

				// If adding to existing batch then task is joined with a batch future so set done future
				// Note that this must be done after joining at least one task with the batch future in case all other blockers already finished.
				Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

				wait(taskBucket->finish(tr, task) && setDone);
				return Void();
			}

			// Start moving through the file list and queuing up blocks.  Only queue up to RESTORE_DISPATCH_ADDTASK_SIZE blocks per Dispatch task
			// and target batchSize total per batch but a batch must end on a complete version boundary so exceed the limit if necessary
			// to reach the end of a version of files.
			state std::vector<Future<Key>> addTaskFutures;
			state Version endVersion = files[0].version;
			state int blocksDispatched = 0;
			state int64_t beginBlock = Params.beginBlock().getOrDefault(task);
			state int i = 0;

			for(; i < files.size(); ++i) {
				RestoreConfig::RestoreFile &f = files[i];

				// Here we are "between versions" (prior to adding the first block of the first file of a new version) so this is an opportunity
				// to end the current dispatch batch (which must end on a version boundary) if the batch size has been reached or exceeded
				if(f.version != endVersion && remainingInBatch <= 0) {
					// Next start will be at the first version after endVersion at the first file first block
					++endVersion;
					beginFile = "";
					beginBlock = 0;
					break;
				}

				// Set the starting point for the next task in case we stop inside this file
				endVersion = f.version;
				beginFile = f.fileName;

				state int64_t j = beginBlock * f.blockSize;
				// For each block of the file
				for(; j < f.fileSize; j += f.blockSize) {
					// Stop if we've reached the addtask limit
					if(blocksDispatched == taskBatchSize)
						break;

					//MX:Important: Add a task for each block in the file.
					if(f.isRange) {
						addTaskFutures.push_back(RestoreRangeTaskFunc::addTask(tr, taskBucket, task,
							f, j, std::min<int64_t>(f.blockSize, f.fileSize - j),
							TaskCompletionKey::joinWith(allPartsDone)));
					}
					else {
						addTaskFutures.push_back(RestoreLogDataTaskFunc::addTask(tr, taskBucket, task,
							f, j, std::min<int64_t>(f.blockSize, f.fileSize - j),
							TaskCompletionKey::joinWith(allPartsDone)));
					}

					// Increment beginBlock for the file and total blocks dispatched for this task
					++beginBlock;
					++blocksDispatched;
					--remainingInBatch;
				}

				// Stop if we've reached the addtask limit
				if(blocksDispatched == taskBatchSize)
					break;

				// We just completed an entire file so the next task should start at the file after this one within endVersion (or later)
				// if this iteration ends up being the last for this task
				beginFile = beginFile + '\x00';
				beginBlock = 0;


				TraceEvent("FileRestoreDispatchedFile")
						.detail("MX", 1)
						.detail("RestoreUID", restore.getUid())
						.detail("FileName", f.fileName)
						.detail("TaskInstance", (uint64_t)this)
						.detail("FileInfo", f.toString());

//				TraceEvent("FileRestoreDispatchedFile")
//					.suppressFor(60)
//					.detail("RestoreUID", restore.getUid())
//					.detail("FileName", f.fileName)
//					.detail("TaskInstance", (uint64_t)this);
			}

			// If no blocks were dispatched then the next dispatch task should run now and be joined with the allPartsDone future
			if(blocksDispatched == 0) {
				std::string decision;

				// If no files were dispatched either then the batch size wasn't large enough to catch all of the files at the next lowest non-dispatched
				// version, so increase the batch size.
				if(i == 0) {
					batchSize *= 2;
					decision = "increased_batch_size";
				}
				else
					decision = "all_files_were_empty";

				TraceEvent("FileRestoreDispatch")
					.detail("RestoreUID", restore.getUid())
					.detail("BeginVersion", beginVersion)
					.detail("BeginFile", printable(Params.beginFile().get(task)))
					.detail("BeginBlock", Params.beginBlock().get(task))
					.detail("EndVersion", endVersion)
					.detail("ApplyLag", applyLag)
					.detail("BatchSize", batchSize)
					.detail("Decision", decision)
					.detail("TaskInstance", (uint64_t)this)
					.detail("RemainingInBatch", remainingInBatch);

				wait(success(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, remainingInBatch, TaskCompletionKey::joinWith((allPartsDone)))));

				// If adding to existing batch then task is joined with a batch future so set done future.
				// Note that this must be done after joining at least one task with the batch future in case all other blockers already finished.
				Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

				wait(setDone && taskBucket->finish(tr, task));

				return Void();
			}

			// Increment the number of blocks dispatched in the restore config
			restore.filesBlocksDispatched().atomicOp(tr, blocksDispatched, MutationRef::Type::AddValue);

			// If beginFile is not empty then we had to stop in the middle of a version (possibly within a file) so we cannot end
			// the batch here because we do not know if we got all of the files and blocks from the last version queued, so
			// make sure remainingInBatch is at least 1.
			if(!beginFile.empty())
				remainingInBatch = std::max<int64_t>(1, remainingInBatch);

			// If more blocks need to be dispatched in this batch then add a follow-on task that is part of the allPartsDone group which will won't wait
			// to run and will add more block tasks.
			if(remainingInBatch > 0)
				addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, remainingInBatch, TaskCompletionKey::joinWith(allPartsDone)));
			else // Otherwise, add a follow-on task to continue after all previously dispatched blocks are done
				addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, 0, TaskCompletionKey::noSignal(), allPartsDone));

			wait(waitForAll(addTaskFutures));

			// If adding to existing batch then task is joined with a batch future so set done future.
			Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

			wait(setDone && taskBucket->finish(tr, task));

			TraceEvent("FileRestoreDispatch")
				.detail("RestoreUID", restore.getUid())
				.detail("BeginVersion", beginVersion)
				.detail("BeginFile", printable(Params.beginFile().get(task)))
				.detail("BeginBlock", Params.beginBlock().get(task))
				.detail("EndVersion", endVersion)
				.detail("ApplyLag", applyLag)
				.detail("BatchSize", batchSize)
				.detail("Decision", "dispatched_files")
				.detail("FilesDispatched", i)
				.detail("BlocksDispatched", blocksDispatched)
				.detail("TaskInstance", (uint64_t)this)
				.detail("RemainingInBatch", remainingInBatch);

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Version beginVersion, std::string beginFile, int64_t beginBlock, int64_t batchSize, int64_t remainingInBatch = 0, TaskCompletionKey completionKey = TaskCompletionKey::noSignal(), Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));

			// Use high priority for dispatch tasks that have to queue more blocks for the current batch
			unsigned int priority = (remainingInBatch > 0) ? 1 : 0;
			state Reference<Task> task(new Task(RestoreDispatchTaskFunc::name, RestoreDispatchTaskFunc::version, doneKey, priority));

			// Create a config from the parent task and bind it to the new task
			wait(RestoreConfig(parentTask).toTask(tr, task));
			Params.beginVersion().set(task, beginVersion);
			Params.batchSize().set(task, batchSize);
			Params.remainingInBatch().set(task, remainingInBatch);
			Params.beginBlock().set(task, beginBlock);
			Params.beginFile().set(task, beginFile);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreDispatchTaskFunc::name = LiteralStringRef("restore_dispatch");
	const uint32_t RestoreDispatchTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreDispatchTaskFunc);

	ACTOR Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state std::vector<KeyBackedTag> tags;
		if(tagName.size() == 0) {
			std::vector<KeyBackedTag> t = wait(getAllRestoreTags(tr));
			tags = t;
		}
		else
			tags.push_back(makeRestoreTag(tagName.toString()));

		state std::string result;
		state int i = 0;

		for(; i < tags.size(); ++i) {
			UidAndAbortedFlagT u = wait(tags[i].getD(tr));
			std::string s = wait(RestoreConfig(u.first).getFullStatus(tr));
			result.append(s);
			result.append("\n\n");
		}

		return result;
	}

	ACTOR Future<ERestoreState> abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		state KeyBackedTag tag = makeRestoreTag(tagName.toString());
		state Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
		if(!current.present())
			return ERestoreState::UNITIALIZED;

		state RestoreConfig restore(current.get().first);

		state ERestoreState status = wait(restore.stateEnum().getD(tr));
		state bool runnable = wait(restore.isRunnable(tr));

		if (!runnable)
			return status;

		restore.stateEnum().set(tr, ERestoreState::ABORTED);

		// Clear all of the ApplyMutations stuff
		restore.clearApplyMutationsKeys(tr);

		// Cancel the backup tasks on this tag
		wait(tag.cancel(tr));
		wait(unlockDatabase(tr, current.get().first));
		return ERestoreState::ABORTED;
	}

	ACTOR Future<ERestoreState> abortRestore(Database cx, Key tagName) {
		state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>( new ReadYourWritesTransaction(cx) );

		loop {
			try {
				ERestoreState estate = wait( abortRestore(tr, tagName) );
				if(estate != ERestoreState::ABORTED) {
					return estate;
				}
				wait(tr->commit());
				break;
			} catch( Error &e ) {
				wait( tr->onError(e) );
			}
		}
		
		tr = Reference<ReadYourWritesTransaction>( new ReadYourWritesTransaction(cx) );

		//Commit a dummy transaction before returning success, to ensure the mutation applier has stopped submitting mutations
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
				tr->addReadConflictRange(singleKeyRange(KeyRef()));
				tr->addWriteConflictRange(singleKeyRange(KeyRef()));
				wait(tr->commit());
				return ERestoreState::ABORTED;
			} catch( Error &e ) {
				wait( tr->onError(e) );
			}
		}
	}

	struct StartFullRestoreTaskFunc : RestoreTaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<Version> firstVersion() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			state RestoreConfig restore(task);
			state Version restoreVersion;
			state Reference<IBackupContainer> bc;

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					wait(checkTaskVersion(tr->getDatabase(), task, name, version));
					Version _restoreVersion = wait(restore.restoreVersion().getOrThrow(tr));
					restoreVersion = _restoreVersion;
					wait(taskBucket->keepRunning(tr, task));

					ERestoreState oldState = wait(restore.stateEnum().getD(tr));
					if(oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
						wait(restore.logError(cx, restore_error(), format("StartFullRestore: Encountered unexpected state(%d)", oldState), this));
						return Void();
					}
					restore.stateEnum().set(tr, ERestoreState::STARTING);
					restore.fileSet().clear(tr);
					restore.fileBlockCount().clear(tr);
					restore.fileCount().clear(tr);
					Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
					bc = _bc;

					wait(tr->commit());
					break;
				} catch(Error &e) {
					wait(tr->onError(e));
				}
			}

			Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(restoreVersion));

			if(!restorable.present())
				throw restore_missing_data();

			// First version for which log data should be applied
			Params.firstVersion().set(task, restorable.get().snapshot.beginVersion);

			// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
			// Order does not matter, they will be put in order when written to the restoreFileMap below.
			state std::vector<RestoreConfig::RestoreFile> files;

			for(const RangeFile &f : restorable.get().ranges) {
				files.push_back({f.version, f.fileName, true, f.blockSize, f.fileSize});
			}
			for(const LogFile &f : restorable.get().logs) {
				files.push_back({f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion});
			}

			state std::vector<RestoreConfig::RestoreFile>::iterator start = files.begin();
			state std::vector<RestoreConfig::RestoreFile>::iterator end = files.end();

			tr->reset();
			while(start != end) {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					wait(taskBucket->keepRunning(tr, task));

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

					TraceEvent("FileRestoreLoadedFiles")
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

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);

			state Version firstVersion = Params.firstVersion().getOrDefault(task, invalidVersion);
			if(firstVersion == invalidVersion) {
				wait(restore.logError(tr->getDatabase(), restore_missing_data(), "StartFullRestore: The backup had no data.", this));
				std::string tag = wait(restore.tag().getD(tr));
				ERestoreState _ = wait(abortRestore(tr, StringRef(tag)));
				return Void();
			}

			restore.stateEnum().set(tr, ERestoreState::RUNNING);

			// Set applyMutation versions
			restore.setApplyBeginVersion(tr, firstVersion);
			restore.setApplyEndVersion(tr, firstVersion);

			// Apply range data and log data in order
			Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, 0, "", 0, CLIENT_KNOBS->RESTORE_DISPATCH_BATCH_SIZE));

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, UID uid, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>())
		{
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(StartFullRestoreTaskFunc::name, StartFullRestoreTaskFunc::version, doneKey));
			TraceEvent("ParalleRestore").detail("AddRestoreTask", task->toString());

			state RestoreConfig restore(uid);
			// Bind the restore config to the new task
			wait(restore.toTask(tr, task));
			TraceEvent("ParalleRestore").detail("AddRestoreConfigToRestoreTask", task->toString());

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef StartFullRestoreTaskFunc::name = LiteralStringRef("restore_start");
	const uint32_t StartFullRestoreTaskFunc::version = 1;
	REGISTER_TASKFUNC(StartFullRestoreTaskFunc);
}

struct LogInfo : public ReferenceCounted<LogInfo> {
	std::string fileName;
	Reference<IAsyncFile> logFile;
	Version beginVersion;
	Version endVersion;
	int64_t offset;

	LogInfo() : offset(0) {};
};

class FileBackupAgentImpl {
public:
	static const int MAX_RESTORABLE_FILE_METASECTION_BYTES = 1024 * 8;

	// This method will return the final status of the backup
	ACTOR static Future<int> waitBackup(FileBackupAgent* backupAgent, Database cx, std::string tagName, bool stopWhenDone) {
		state std::string backTrace;
		state KeyBackedTag tag = makeBackupTag(tagName);

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
				if (!oldUidAndAborted.present()) {
					return EBackupState::STATE_NEVERRAN;
				}

				state BackupConfig config(oldUidAndAborted.get().first);
				state EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));

				// Break, if no longer runnable
				if (!FileBackupAgent::isRunnable(status)) {
					return status;
				}

				// Break, if in differential mode (restorable) and stopWhenDone is not enabled
				if ((!stopWhenDone) && (BackupAgentBase::STATE_DIFFERENTIAL == status)) {
					return status;
				}

				state Future<Void> watchFuture = tr->watch( config.stateEnum().key );
				wait( tr->commit() );
				wait( watchFuture );
			}
			catch (Error &e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> submitBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key outContainer, int snapshotIntervalSeconds, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		TraceEvent(SevInfo, "FBA_SubmitBackup")
				.detail("TagName", tagName.c_str())
				.detail("StopWhenDone", stopWhenDone)
				.detail("OutContainer", outContainer.toString());

		state KeyBackedTag tag = makeBackupTag(tagName);
		Optional<UidAndAbortedFlagT> uidAndAbortedFlag = wait(tag.get(tr));
		if (uidAndAbortedFlag.present()) {
			state BackupConfig prevConfig(uidAndAbortedFlag.get().first);
			state EBackupState prevBackupStatus = wait(prevConfig.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));
			if (FileBackupAgent::isRunnable(prevBackupStatus)) {
				throw backup_duplicate();
			}

			// Now is time to clear prev backup config space. We have no more use for it.
			prevConfig.clear(tr);
		}

		state BackupConfig config(g_random->randomUniqueID());
		state UID uid = config.getUid();

		// This check will ensure that current backupUid is later than the last backup Uid
		state Standalone<StringRef> nowStr = BackupAgentBase::getCurrentTime();
		state std::string backupContainer = outContainer.toString();

		// To be consistent with directory handling behavior since FDB backup was first released, if the container string
		// describes a local directory then "/backup-<timestamp>" will be added to it.
		if(backupContainer.find("file://") == 0) {
			backupContainer = joinPath(backupContainer, std::string("backup-") + nowStr.toString());
		}

		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupContainer);
		try {
			wait(timeoutError(bc->create(), 30));
		} catch(Error &e) {
			if(e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: Could not create backup container: %s\n", e.what());
			throw backup_error();
		}

		Optional<Value> lastBackupTimestamp = wait(backupAgent->lastBackupTimestamp().get(tr));

		if ((lastBackupTimestamp.present()) && (lastBackupTimestamp.get() >= nowStr)) {
			fprintf(stderr, "ERROR: The last backup `%s' happened in the future.\n", printable(lastBackupTimestamp.get()).c_str());
			throw backup_error();
		}

		KeyRangeMap<int> backupRangeSet;
		for (auto& backupRange : backupRanges) {
			backupRangeSet.insert(backupRange, 1);
		}

		backupRangeSet.coalesce(allKeys);
		state std::vector<KeyRange> normalizedRanges;

		for (auto& backupRange : backupRangeSet.ranges()) {
			if (backupRange.value()) {
				normalizedRanges.push_back(KeyRange(KeyRangeRef(backupRange.range().begin, backupRange.range().end)));
			}
		}

		config.clear(tr);

		state Key destUidValue(BinaryWriter::toValue(uid, Unversioned()));
		if (normalizedRanges.size() == 1) {
			state Key destUidLookupPath = BinaryWriter::toValue(normalizedRanges[0], IncludeVersion()).withPrefix(destUidLookupPrefix);
			Optional<Key> existingDestUidValue = wait(tr->get(destUidLookupPath));
			if (existingDestUidValue.present()) {
				destUidValue = existingDestUidValue.get();
			} else {
				destUidValue = BinaryWriter::toValue(g_random->randomUniqueID(), Unversioned());
				tr->set(destUidLookupPath, destUidValue);
			}
		}

		tr->set(config.getUidAsKey().withPrefix(destUidValue).withPrefix(backupLatestVersionsPrefix), BinaryWriter::toValue<Version>(tr->getReadVersion().get(), Unversioned()));
		config.destUidValue().set(tr, destUidValue);

		// Point the tag to this new uid
		tag.set(tr, {uid, false});

		backupAgent->lastBackupTimestamp().set(tr, nowStr);

		// Set the backup keys
		config.tag().set(tr, tagName);
		config.stateEnum().set(tr, EBackupState::STATE_SUBMITTED);
		config.backupContainer().set(tr, bc);
		config.stopWhenDone().set(tr, stopWhenDone);
		config.backupRanges().set(tr, normalizedRanges);
		config.snapshotIntervalSeconds().set(tr, snapshotIntervalSeconds);

		Key taskKey = wait(fileBackup::StartFullBackupTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

		return Void();
	}

	//MX: All restore code; BackupContainer: format of backup file.
	ACTOR static Future<Void> submitRestore(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName, Key backupURL, Version restoreVersion, Key addPrefix, Key removePrefix, KeyRange restoreRange, bool lockDB, UID uid) {
		ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// Get old restore config for this tag
		state KeyBackedTag tag = makeRestoreTag(tagName.toString());
		state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
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

		Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

		if (lockDB)
			wait(lockDatabase(tr, uid));
		else
			wait(checkDatabaseLock(tr, uid));

		return Void();
	}

	// This method will return the final status of the backup
	ACTOR static Future<ERestoreState> waitRestore(Database cx, Key tagName, bool verbose) {
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

	ACTOR static Future<Void> discontinueBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName.toString());
		state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, false, backup_unneeded()));
		state BackupConfig config(current.first);
		state EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));

		if (!FileBackupAgent::isRunnable(status)) {
			throw backup_unneeded();
		}

		// If the backup is already restorable then 'mostly' abort it - cancel all tasks via the tag 
		// and clear the mutation logging config and data - but set its state as COMPLETED instead of ABORTED.
		state Optional<Version> latestRestorableVersion = wait(config.getLatestRestorableVersion(tr));

		TraceEvent(SevInfo, "FBA_DiscontinueBackup")
				.detail("AlreadyRestorable", latestRestorableVersion.present() ? "Yes" : "No")
				.detail("TagName", tag.tagName.c_str())
				.detail("Status", BackupAgentBase::getStateText(status));

		if(latestRestorableVersion.present()) {
			// Cancel all backup tasks through tag
			wait(tag.cancel(tr));

			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

			state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
			state Version endVersion = wait(tr->getReadVersion());

			Key _ = wait(fileBackup::EraseLogRangeTaskFunc::addTask(tr, backupAgent->taskBucket, config.getUid(), TaskCompletionKey::noSignal(), destUidValue));

			config.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

			return Void();
		}

		state bool stopWhenDone = wait(config.stopWhenDone().getOrThrow(tr));

		if (stopWhenDone) {
			throw backup_duplicate();
		}

		config.stopWhenDone().set(tr, true);

		return Void();
	}

	ACTOR static Future<Void> abortBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, std::string tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName);
		state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, false, backup_unneeded()));

		state BackupConfig config(current.first);
		state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
		EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));

		if (!backupAgent->isRunnable((BackupAgentBase::enumState)status)) {
			throw backup_unneeded();
		}

		TraceEvent(SevInfo, "FBA_AbortBackup")
				.detail("TagName", tagName.c_str())
				.detail("Status", BackupAgentBase::getStateText(status));

		// Cancel backup task through tag
		wait(tag.cancel(tr));

		Key _ = wait(fileBackup::EraseLogRangeTaskFunc::addTask(tr, backupAgent->taskBucket, config.getUid(), TaskCompletionKey::noSignal(), destUidValue));

		config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

		return Void();
	}

	ACTOR static Future<std::string> getStatus(FileBackupAgent* backupAgent, Database cx, bool showErrors, std::string tagName) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state std::string statusText;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag;
				state BackupConfig config;
				state EBackupState backupState;

				statusText = "";
				tag = makeBackupTag(tagName);
				state Optional<UidAndAbortedFlagT> uidAndAbortedFlag = wait(tag.get(tr));
				state Future<Optional<Value>> fPaused = tr->get(backupAgent->taskBucket->getPauseKey());
				if (uidAndAbortedFlag.present()) {
					config = BackupConfig(uidAndAbortedFlag.get().first);
					EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));
					backupState = status;
				}

				if (!uidAndAbortedFlag.present() || backupState == EBackupState::STATE_NEVERRAN) {
					statusText += "No previous backups found.\n";
				} else {
					state std::string backupStatus(BackupAgentBase::getStateText(backupState));
					state Reference<IBackupContainer> bc;
					state Optional<Version> latestRestorableVersion;
					state Version recentReadVersion;
					
					wait( store(config.getLatestRestorableVersion(tr), latestRestorableVersion)
								&& store(config.backupContainer().getOrThrow(tr), bc)
								&& store(tr->getReadVersion(), recentReadVersion)
							);

					bool snapshotProgress = false;

					switch (backupState) {
						case BackupAgentBase::STATE_SUBMITTED:
							statusText += "The backup on tag `" + tagName + "' is in progress (just started) to " + bc->getURL() + ".\n";
							break;
						case BackupAgentBase::STATE_BACKUP:
							statusText += "The backup on tag `" + tagName + "' is in progress to " + bc->getURL() + ".\n";
							snapshotProgress = true;
							break;
						case BackupAgentBase::STATE_DIFFERENTIAL:
							statusText += "The backup on tag `" + tagName + "' is restorable but continuing to " + bc->getURL() + ".\n";
							snapshotProgress = true;
							break;
						case BackupAgentBase::STATE_COMPLETED:
							statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() + " completed at version " + format("%lld", latestRestorableVersion.orDefault(-1)) + ".\n";
							break;
						default:
							statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() + " " + backupStatus + ".\n";
							break;
					}

					if(snapshotProgress) {
						state int64_t snapshotInterval;
						state Version snapshotBeginVersion;
						state Version snapshotTargetEndVersion;
						state Optional<Version> latestSnapshotEndVersion;
						state Optional<Version> latestLogEndVersion;
						state Optional<int64_t> logBytesWritten;
						state Optional<int64_t> rangeBytesWritten;
						state Optional<int64_t> latestSnapshotEndVersionTimestamp;
						state Optional<int64_t> latestLogEndVersionTimestamp;
						state Optional<int64_t> snapshotBeginVersionTimestamp;
						state Optional<int64_t> snapshotTargetEndVersionTimestamp;
						state bool stopWhenDone;

						wait( store(config.snapshotBeginVersion().getOrThrow(tr), snapshotBeginVersion)
									&& store(config.snapshotTargetEndVersion().getOrThrow(tr), snapshotTargetEndVersion)
									&& store(config.snapshotIntervalSeconds().getOrThrow(tr), snapshotInterval)
									&& store(config.logBytesWritten().get(tr), logBytesWritten)
									&& store(config.rangeBytesWritten().get(tr), rangeBytesWritten)
									&& store(config.latestLogEndVersion().get(tr), latestLogEndVersion)
									&& store(config.latestSnapshotEndVersion().get(tr), latestSnapshotEndVersion)
									&& store(config.stopWhenDone().getOrThrow(tr), stopWhenDone) 
									);

						wait( store(getTimestampFromVersion(latestSnapshotEndVersion, tr), latestSnapshotEndVersionTimestamp)
									&& store(getTimestampFromVersion(latestLogEndVersion, tr), latestLogEndVersionTimestamp)
									&& store(timeKeeperEpochsFromVersion(snapshotBeginVersion, tr), snapshotBeginVersionTimestamp)
									&& store(timeKeeperEpochsFromVersion(snapshotTargetEndVersion, tr), snapshotTargetEndVersionTimestamp)
									);

						statusText += format("Snapshot interval is %lld seconds.  ", snapshotInterval);
						if(backupState == BackupAgentBase::STATE_DIFFERENTIAL)
							statusText += format("Current snapshot progress target is %3.2f%% (>100%% means the snapshot is supposed to be done)\n", 100.0 * (recentReadVersion - snapshotBeginVersion) / (snapshotTargetEndVersion - snapshotBeginVersion)) ;
						else
							statusText += "The initial snapshot is still running.\n";
						
						statusText += format("\nDetails:\n LogBytes written - %ld\n RangeBytes written - %ld\n "
											 "Last complete log version and timestamp        - %s, %s\n "
											 "Last complete snapshot version and timestamp   - %s, %s\n "
											 "Current Snapshot start version and timestamp   - %s, %s\n "
											 "Expected snapshot end version and timestamp    - %s, %s\n "
											 "Backup supposed to stop at next snapshot completion - %s\n",
											 logBytesWritten.orDefault(0), rangeBytesWritten.orDefault(0),
											 versionToString(latestLogEndVersion).c_str(), timeStampToString(latestLogEndVersionTimestamp).c_str(),
											 versionToString(latestSnapshotEndVersion).c_str(), timeStampToString(latestSnapshotEndVersionTimestamp).c_str(),
											 versionToString(snapshotBeginVersion).c_str(), timeStampToString(snapshotBeginVersionTimestamp).c_str(),
											 versionToString(snapshotTargetEndVersion).c_str(), timeStampToString(snapshotTargetEndVersionTimestamp).c_str(),
											 boolToYesOrNo(stopWhenDone).c_str());
					}

					// Append the errors, if requested
					if (showErrors) {
						KeyBackedMap<int64_t, std::pair<std::string, Version>>::PairsType errors = wait(config.lastErrorPerType().getRange(tr, 0, std::numeric_limits<int>::max(), CLIENT_KNOBS->TOO_MANY));
						std::string recentErrors;
						std::string pastErrors;

						for(auto &e : errors) {
							Version v = e.second.second;
							std::string msg = format("%s ago : %s\n", secondsToTimeFormat((recentReadVersion - v) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND).c_str(), e.second.first.c_str());

							// If error version is at or more recent than the latest restorable version then it could be inhibiting progress
							if(v >= latestRestorableVersion.orDefault(0)) {
								recentErrors += msg;
							}
							else {
								pastErrors += msg;
							}
						}

						if (!recentErrors.empty()) {
							if (latestRestorableVersion.present())
								statusText += format("Recent Errors (since latest restorable point %s ago)\n",
														secondsToTimeFormat((recentReadVersion - latestRestorableVersion.get()) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND).c_str())
											  + recentErrors;
							else
								statusText += "Recent Errors (since initialization)\n" + recentErrors;
						}
						if(!pastErrors.empty())
							statusText += "Older Errors\n" + pastErrors;
					}
				}

				Optional<Value> paused = wait(fPaused);
				if(paused.present()) {
					statusText += format("\nAll backup agents have been paused.\n");
				}

				break;
			}
			catch (Error &e) {
				wait(tr->onError(e));
			}
		}

		return statusText;
	}

	ACTOR static Future<Version> getLastRestorable(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> version = wait(tr->get(backupAgent->lastRestorable.pack(tagName)));

		return (version.present()) ? BinaryReader::fromStringRef<Version>(version.get(), Unversioned()) : 0;
	}

	static StringRef read(StringRef& data, int bytes) {
		if (bytes > data.size()) throw restore_error();
		StringRef r = data.substr(0, bytes);
		data = data.substr(bytes);
		return r;
	}

	ACTOR static Future<Version> restore_old(FileBackupAgent* backupAgent, Database cx, Key tagName, Key url, bool waitForComplete, Version targetVersion, bool verbose, KeyRange range, Key addPrefix, Key removePrefix, bool lockDB, UID randomUid) {

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

		if(!restoreSet.present()) {
			TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
				.detail("BackupContainer", bc->getURL())
				.detail("TargetVersion", targetVersion);
			fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
			throw restore_invalid_version();
		}

		if (verbose) {
			printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		}

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(submitRestore(backupAgent, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid));
				wait(tr->commit());
				//MX: restore agent example
				//TODO: MX: add restore master
				//printf("MX:Perform FileBackupAgent restore...\n");
				//Future<Void> ra1 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());
				//Future<Void> ra2 = restoreAgent_run(cx.getPtr()->cluster->getConnectionFile(), LocalityData());

				break;
			} catch(Error &e) {
				if(e.code() != error_code_restore_duplicate_tag) {
					wait(tr->onError(e));
				}
			}
		}

		if(waitForComplete) {
			ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
			if(finalState != ERestoreState::COMPLETED)
				throw restore_error();
		}

		return targetVersion;
	}

	//used for correctness only, locks the database before discontinuing the backup and that same lock is then used while doing the restore.
	//the tagname of the backup must be the same as the restore.
	ACTOR static Future<Version> atomicRestore(FileBackupAgent* backupAgent, Database cx, Key tagName, KeyRange range, Key addPrefix, Key removePrefix) {
		state Reference<ReadYourWritesTransaction> ryw_tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
		state BackupConfig backupConfig;
		loop {
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state KeyBackedTag tag = makeBackupTag(tagName.toString());
				UidAndAbortedFlagT uidFlag = wait(tag.getOrThrow(ryw_tr));
				backupConfig = BackupConfig(uidFlag.first);
				state EBackupState status = wait(backupConfig.stateEnum().getOrThrow(ryw_tr));

				if (status != BackupAgentBase::STATE_DIFFERENTIAL ) {
					throw backup_duplicate();
				}

				break;
			} catch( Error &e ) {
				wait( ryw_tr->onError(e) );
			}
		}
		
		//Lock src, record commit version
		state Transaction tr(cx);
		state Version commitVersion;
		state UID randomUid = g_random->randomUniqueID();
		loop {
			try {
				// We must get a commit version so add a conflict range that won't likely cause conflicts
				// but will ensure that the transaction is actually submitted.
				tr.addWriteConflictRange(backupConfig.snapshotRangeDispatchMap().space.range());
				wait( lockDatabase(&tr, randomUid) );
				wait(tr.commit());
				commitVersion = tr.getCommittedVersion();
				TraceEvent("AS_Locked").detail("CommitVer", commitVersion);
				break;
			} catch( Error &e ) {
				wait(tr.onError(e));
			}
		}

		ryw_tr->reset();
		loop {
			try {
				Optional<Version> restoreVersion = wait( backupConfig.getLatestRestorableVersion(ryw_tr) );
				if(restoreVersion.present() && restoreVersion.get() >= commitVersion) {
					TraceEvent("AS_RestoreVersion").detail("RestoreVer", restoreVersion.get());
					break;
				} else {
					ryw_tr->reset();
					wait(delay(0.2));
				}
			} catch( Error &e ) {
				wait( ryw_tr->onError(e) );
			}
		}

		ryw_tr->reset();
		loop {
			try {
				wait( discontinueBackup(backupAgent, ryw_tr, tagName) );
				wait( ryw_tr->commit() );
				TraceEvent("AS_DiscontinuedBackup");
				break;
			} catch( Error &e ) {
				if(e.code() == error_code_backup_unneeded || e.code() == error_code_backup_duplicate){
					break;
				}
				wait( ryw_tr->onError(e) );
			}
		}

		int _ = wait( waitBackup(backupAgent, cx, tagName.toString(), true) );
		TraceEvent("AS_BackupStopped");

		ryw_tr->reset();
		loop {
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);	
				ryw_tr->addReadConflictRange(range);
				ryw_tr->clear(range);
				wait( ryw_tr->commit() );
				TraceEvent("AS_ClearedRange");
				break;
			} catch( Error &e ) {
				wait( ryw_tr->onError(e) );
			}
		}

		Reference<IBackupContainer> bc = wait(backupConfig.backupContainer().getOrThrow(cx));

		TraceEvent("AS_StartRestore");
		Version ver = wait( restore(backupAgent, cx, tagName, KeyRef(bc->getURL()), true, -1, true, range, addPrefix, removePrefix, true, randomUid) );
		return ver;
	}


	ACTOR static Future<Version> restore(FileBackupAgent* backupAgent, Database cx, Key tagName, Key url, bool waitForComplete, Version targetVersion, bool verbose, KeyRange range, Key addPrefix, Key removePrefix, bool lockDB, UID randomUid) {

		Version ver = wait( restore_old(backupAgent, cx, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix, lockDB, randomUid) );
		return ver;
//
//		TraceEvent("WaitOnRestoreAgentFutureBegin").detail("Actor", "RestoreAgentRestore")
//				.detail("URL", url.contents().printable());
//
//		Future<Void> v1 = restoreAgentRestore(backupAgent, cx, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix, lockDB, randomUid);
//		Future<Void> v2 = restoreAgentRestore(backupAgent, cx, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix, lockDB, randomUid);
//
//		wait(v1 || v2);
//
//		TraceEvent("WaitOnRestoreAgentFutureEnd").detail("Actor", "RestoreAgentRestore")
//				.detail("URL", url.contents().printable());
//
//		return targetVersion;

	}

};

const std::string BackupAgentBase::defaultTagName = "default";
const int BackupAgentBase::logHeaderSize = 12;
const int FileBackupAgent::dataFooterSize = 20;

Future<Version> FileBackupAgent::restore(Database cx, Key tagName, Key url, bool waitForComplete, Version targetVersion, bool verbose, KeyRange range, Key addPrefix, Key removePrefix, bool lockDB) {
	return FileBackupAgentImpl::restore(this, cx, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix, lockDB, g_random->randomUniqueID());
}

Future<Version> FileBackupAgent::atomicRestore(Database cx, Key tagName, KeyRange range, Key addPrefix, Key removePrefix) {
	return FileBackupAgentImpl::atomicRestore(this, cx, tagName, range, addPrefix, removePrefix);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::abortRestore(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Database cx, Key tagName) {
	return fileBackup::abortRestore(cx, tagName);
}

Future<std::string> FileBackupAgent::restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::restoreStatus(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::waitRestore(Database cx, Key tagName, bool verbose) {
	return FileBackupAgentImpl::waitRestore(cx, tagName, verbose);
};

Future<Void> FileBackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr, Key outContainer, int snapshotIntervalSeconds, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone) {
	return FileBackupAgentImpl::submitBackup(this, tr, outContainer, snapshotIntervalSeconds, tagName, backupRanges, stopWhenDone);
}

Future<Void> FileBackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName){
	return FileBackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void> FileBackupAgent::abortBackup(Reference<ReadYourWritesTransaction> tr, std::string tagName){
	return FileBackupAgentImpl::abortBackup(this, tr, tagName);
}

Future<std::string> FileBackupAgent::getStatus(Database cx, bool showErrors, std::string tagName) {
	return FileBackupAgentImpl::getStatus(this, cx, showErrors, tagName);
}

Future<Version> FileBackupAgent::getLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return FileBackupAgentImpl::getLastRestorable(this, tr, tagName);
}

void FileBackupAgent::setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->set(lastRestorable.pack(tagName), BinaryWriter::toValue<Version>(version, Unversioned()));
}

Future<int> FileBackupAgent::waitBackup(Database cx, std::string tagName, bool stopWhenDone) {
	return FileBackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone);
}



//-------MX: Parallel restore code

void traceRestorableFileSet(Optional<RestorableFileSet> restoreSetOpt) {
	if ( restoreSetOpt.present() ) {
		return;
	}
	RestorableFileSet &restoreSet = restoreSetOpt.get();
	TraceEvent("RestorableFileSet").detail("Version", restoreSet.targetVersion);
	int i = 0;
	for ( auto &log : restoreSet.logs ) {
		TraceEvent("LogFile\t").detail("Index", i++).detail("Info", log.toString());
	}
	i = 0;
	for ( auto &range : restoreSet.ranges ) {
		TraceEvent("RangeFile\t").detail("Index", i++).detail("Info", range.toString());
	}

	TraceEvent("Snapshot\t").detail("Info", restoreSet.snapshot.toString());
}

ACTOR Future<Void> restoreAgentRestore(FileBackupAgent* backupAgent, Database db, Standalone<StringRef>  tagName, Standalone<StringRef>  url, bool waitForComplete,
										  Version targetVersion, bool verbose, Standalone<KeyRangeRef> range, Standalone<StringRef>  addPrefix, Standalone<StringRef>  removePrefix, bool lockDB, UID randomUid) {

	TraceEvent("RestoreAgentRestoreRun").detail("URL", url.contents().printable());
	state Database cx = db;

	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	TraceEvent("RestoreAgentStartTryBeLeader");
	state Transaction tr1(cx);
	TraceEvent("RestoreAgentStartCreateTransaction").detail("NumErrors", tr1.numErrors);
	loop {
		try {
			TraceEvent("RestoreAgentStartReadLeaderKeyStart").detail("NumErrors", tr1.numErrors).detail("ReadLeaderKey", restoreLeaderKey.printable());
			Optional<Value> leader = wait(tr1.get(restoreLeaderKey));
			TraceEvent("RestoreAgentStartReadLeaderKeyEnd").detail("NumErrors", tr1.numErrors)
					.detail("LeaderValuePresent", leader.present());
			if(leader.present()) {
				leaderInterf = decodeRestoreAgentValue(leader.get());
				break;
			}
			tr1.set(restoreLeaderKey, restoreAgentValue(interf));
			wait(tr1.commit());
			break;
		} catch( Error &e ) {
			wait( tr1.onError(e) );
		}
	}


	//NOTE: leader may die, when that happens, all agents will block. We will have to clear the leader key and launch a new leader
	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		printf("MX: I am NOT the leader.\n");
		TraceEvent("RestoreAgentNotLeader");
		loop {
			try {
				tr1.set(restoreAgentKeyFor(interf.id()), restoreAgentValue(interf));
				wait(tr1.commit());
				break;
			} catch( Error &e ) {
				wait( tr1.onError(e) );
			}
		}

		loop {
			choose {
				//Dumpy code from skeleton code
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

					if ( values[req.testData] > 2 ) { // set > 0 to skip restore; set > 2 to enable restore
						TraceEvent("DoNotRunRestoreTwice");
						continue;
					}

					//MX: actual restore from old restore code
					state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
					loop {
						try {
							tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
							tr->setOption(FDBTransactionOptions::LOCK_AWARE);
							wait(FileBackupAgentImpl::submitRestore(backupAgent, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid));
							wait(tr->commit());

							break;
						} catch(Error &e) {
							if(e.code() != error_code_restore_duplicate_tag) {
								wait(tr->onError(e));
							}
						}
					}

					if(waitForComplete) {
						ERestoreState finalState = wait(FileBackupAgentImpl::waitRestore(cx, tagName, verbose));
						if(finalState != ERestoreState::COMPLETED)
							throw restore_error();
					}

					return Void();
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
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr1.getRange(restoreAgentsKeys, CLIENT_KNOBS->TOO_MANY));
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
			wait( tr1.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	//create initial point, The initial request value for agent i is i.
	state std::vector<int> restoreRequests;
	for ( int i = 0; i < agents.size(); ++i ) {
		restoreRequests.push_back(i);
	}

	loop {
		wait(delay(1.0));

		printf("---Sending Requests\n");
		TraceEvent("RestoreAgentLeader").detail("SendingRequests", "CheckBelow");
		for (int i = 0; i < restoreRequests.size(); ++i ) {
			printf("RestoreRequests[%d]=%d\n", i, restoreRequests[i]);
			TraceEvent("RestoreRequests").detail("Index", i).detail("Value", restoreRequests[i]);
		}
		std::vector<Future<RestoreReply>> replies;
		for ( int i = 0; i < agents.size(); ++i) {
			auto &it = agents[i];
			replies.push_back( it.request.getReply(RestoreRequest(i, restoreRequests)) );
		}
		printf("Wait on all %d requests\n", agents.size());

		std::vector<RestoreReply> reps = wait( getAll(replies ));
		printf("GetRestoreReply values\n", reps.size());
		for ( int i = 0; i < reps.size(); ++i ) {
			printf("RestoreReply[%d]=%d\n [checksum=%d]", i, reps[i].restoreReplies[i], reps[i].replyData);
			//prepare to send the next request batch
			restoreRequests[i] = reps[i].restoreReplies[i];
			if ( restoreRequests[i] > 10 ) { //MX: only calculate up to 10
				return Void();
			}
		}
		TraceEvent("RestoreAgentLeader").detail("GetTestReplySize",  reps.size());

		//-------- Restore Code start -----------------

		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString());
		state BackupDescription desc = wait(bc->describeBackup());

		wait(desc.resolveVersionTimes(cx));

		printf("Backup Description\n%s", desc.toString().c_str());
		printf("MX: Restore master code comes here\n");
		if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
			targetVersion = desc.maxRestorableVersion.get();

		Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));
		TraceEvent("ParallelRestore").detail("MX", "Info").detail("RestoreSet", "Below");
		traceRestorableFileSet(restoreSet);

		if(!restoreSet.present()) {
			TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
					.detail("BackupContainer", bc->getURL())
					.detail("TargetVersion", targetVersion);
			fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
			throw restore_invalid_version();
		}

		if (verbose) {
			printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		}
	}
}

