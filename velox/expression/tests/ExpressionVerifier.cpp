/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/common/base/Fs.h"
#include "velox/expression/Expr.h"
#include "velox/expression/tests/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/parse/TypeResolver.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"

namespace facebook::velox::test {
namespace {

// File names used to persist data required for reproducing a failed test
// case.
static constexpr std::string_view kInputVectorFileName = "input_vector";
static constexpr std::string_view kIndicesOfLazyColumnsFileName =
    "indices_of_lazy_columns";
static constexpr std::string_view kResultVectorFileName = "result_vector";
static constexpr std::string_view kExpressionInSqlFileName = "sql";
static constexpr std::string_view kComplexConstantsFileName =
    "complex_constants";

void logRowVector(const RowVectorPtr& rowVector) {
  if (rowVector == nullptr) {
    return;
  }
  LOG(INFO) << rowVector->childrenSize() << " vectors as input:";
  for (const auto& child : rowVector->children()) {
    LOG(INFO) << "\t" << child->toString(/*recursive=*/true);
  }

  if (VLOG_IS_ON(1)) {
    LOG(INFO) << "RowVector contents (" << rowVector->type()->toString()
              << "):";

    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      LOG(INFO) << "\tAt " << i << ": " << rowVector->toString(i);
    }
  }
}

void compareVectors(const VectorPtr& left, const VectorPtr& right) {
  VELOX_CHECK_EQ(left->size(), right->size());

  // Print vector contents if in verbose mode.
  size_t vectorSize = left->size();
  if (VLOG_IS_ON(1)) {
    LOG(INFO) << "== Result contents (common vs. simple): ";
    for (auto i = 0; i < vectorSize; i++) {
      LOG(INFO) << "At " << i << ": [" << left->toString(i) << " vs "
                << right->toString(i) << "]";
    }
    LOG(INFO) << "===================";
  }

  for (auto i = 0; i < vectorSize; i++) {
    VELOX_CHECK(
        left->equalValueAt(right.get(), i, i),
        "Different results at idx '{}': '{}' vs. '{}'",
        i,
        left->toString(i),
        right->toString(i));
  }
  LOG(INFO) << "All results match.";
}

// Returns a copy of 'rowVector' but with the columns having indices listed in
// 'columnsToWrapInLazy' wrapped in lazy encoding. 'columnsToWrapInLazy'
// should be a sorted list of column indices.
RowVectorPtr wrapColumnsInLazy(
    RowVectorPtr rowVector,
    const std::vector<column_index_t>& columnsToWrapInLazy) {
  if (columnsToWrapInLazy.empty()) {
    return rowVector;
  }
  std::vector<VectorPtr> children;
  int listIndex = 0;
  for (column_index_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    VELOX_USER_CHECK_NOT_NULL(child);
    VELOX_USER_CHECK(!child->isLazy());
    if (listIndex < columnsToWrapInLazy.size() &&
        i == columnsToWrapInLazy[listIndex]) {
      listIndex++;
      child = VectorFuzzer::wrapInLazyVector(child);
    }
    children.push_back(child);
  }

  BufferPtr newNulls = nullptr;
  if (rowVector->nulls()) {
    newNulls = AlignedBuffer::copy(rowVector->pool(), rowVector->nulls());
  }

  auto lazyRowVector = std::make_shared<RowVector>(
      rowVector->pool(),
      rowVector->type(),
      newNulls,
      rowVector->size(),
      std::move(children));
  LOG(INFO) << "Modified inputs for common eval path: ";
  logRowVector(lazyRowVector);
  return lazyRowVector;
}


std::string writeToFile(
    const std::vector<RowVectorPtr>& vectors,
    memory::MemoryPool* FOLLY_NONNULL pool) {
  static const auto kWriter = "HiveConnectorTestBase.Writer";
  std::string filePath =
      exec::test::TempFilePath::create()->path; // get temp file path
  facebook::velox::dwrf::WriterOptions options;
  options.config = std::make_shared<facebook::velox::dwrf::Config>();
  options.schema = vectors[0]->type();
  auto sink =
      std::make_unique<facebook::velox::dwio::common::FileSink>(filePath);
  auto childPool = pool->addChild(kWriter, std::numeric_limits<int64_t>::max());
  facebook::velox::dwrf::Writer writer{options, std::move(sink), *childPool};
  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
  return filePath;
}

std::shared_ptr<connector::ConnectorSplit> makeHiveConnectorSplit(
    const std::string& filePath) {
  return exec::test::HiveConnectorSplitBuilder(filePath)
      .start(0)
      .length(std::numeric_limits<uint64_t>::max())
      .build();
}

void removeFile(const std::string& filename) {
  try {
    if (std::filesystem::remove(filename))
      std::cout << "file " << filename << " deleted.\n";
    else
      std::cout << "file " << filename << " not found.\n";
  } catch (const std::filesystem::filesystem_error& err) {
    std::cout << "filesystem error: " << err.what() << '\n';
  }
}

RowVectorPtr evaluateUsingOperators(
    const core::TypedExprPtr& expr,
    RowVectorPtr rowVector) {
  auto filePath = writeToFile({rowVector}, rowVector->pool());
  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  core::PlanNodeId scanNodeId;
  // functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType)
                  .capturePlanNodeId(scanNodeId)
                  .project({expr})
                  .planNode();
  auto result = exec::test::AssertQueryBuilder(plan)
                    .splits(scanNodeId, {makeHiveConnectorSplit(filePath)})
                    .copyResults(rowVector->pool());
  removeFile(filePath);
  return result;
}

bool hasWriterSupport(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::ROW:
    case TypeKind::MAP:
    case TypeKind::ARRAY:
      for (auto i = 0; i < type->size(); ++i) {
        if(!hasWriterSupport(type->childAt(i))) return false;
      }
      return true;

    default:
      return false;
  }
}

} // namespace

bool ExpressionVerifier::setupReaderWriter = false;

void ExpressionVerifier::setup() {
  if (!setupReaderWriter) {
    setupReaderWriter = true;
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector("test-hive", nullptr);
    connector::registerConnector(hiveConnector);
    // To be able to read local files, we need to register the local file
    // filesystem. We also need to register the dwrf reader factory as well as a
    // write protocol, in this case commit is not required:
    filesystems::registerLocalFileSystem();
    dwrf::registerDwrfReaderFactory();
    connector::hive::HiveNoCommitWriteProtocol::registerProtocol();
  }
}

bool ExpressionVerifier::verify(
    const core::TypedExprPtr& plan,
    const RowVectorPtr& rowVector,
    VectorPtr&& resultVector,
    bool canThrow,
    std::vector<column_index_t> columnsToWrapInLazy) {
  LOG(INFO) << "Executing expression: " << plan->toString();

  logRowVector(rowVector);

  // Store data and expression in case of reproduction.
  VectorPtr copiedResult;
  std::string sql;

  // Complex constants that aren't all expressable in sql
  std::vector<VectorPtr> complexConstants;
  // Deep copy to preserve the initial state of result vector.
  if (!options_.reproPersistPath.empty()) {
    if (resultVector) {
      copiedResult = BaseVector::copy(*resultVector);
    }
    std::vector<core::TypedExprPtr> typedExprs = {plan};
    // Disabling constant folding in order to preserver the original
    // expression
    try {
      sql = exec::ExprSet(std::move(typedExprs), execCtx_, false)
                .expr(0)
                ->toSql(&complexConstants);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to generate SQL: " << e.what();
      sql = "<failed to generate>";
    }
    if (options_.persistAndRunOnce) {
      persistReproInfo(
          rowVector, columnsToWrapInLazy, copiedResult, sql, complexConstants);
    }
  }

  // Execute expression plan using both common and simplified evals.
  std::vector<VectorPtr> commonEvalResult(1);
  std::vector<VectorPtr> simplifiedEvalResult(1);

  commonEvalResult[0] = resultVector;

  std::exception_ptr exceptionCommonPtr;
  std::exception_ptr exceptionSimplifiedPtr;
  std::exception_ptr exceptionOperatorPtr;

  VLOG(1) << "Starting common eval execution.";
  SelectivityVector rows{rowVector ? rowVector->size() : 1};

  // Execute with common expression eval path. Some columns of the input row
  // vector will be wrapped in lazy as specified in 'columnsToWrapInLazy'.
  try {
    exec::ExprSet exprSetCommon(
        {plan}, execCtx_, !options_.disableConstantFolding);
    auto inputRowVector = wrapColumnsInLazy(rowVector, columnsToWrapInLazy);
    exec::EvalCtx evalCtxCommon(execCtx_, &exprSetCommon, inputRowVector.get());

    try {
      exprSetCommon.eval(rows, evalCtxCommon, commonEvalResult);
    } catch (...) {
      if (!canThrow) {
        LOG(ERROR)
            << "Common eval wasn't supposed to throw, but it did. Aborting.";
        throw;
      }
      exceptionCommonPtr = std::current_exception();
    }
  } catch (...) {
    exceptionCommonPtr = std::current_exception();
  }

  VLOG(1) << "Starting simplified eval execution.";

  // Execute with simplified expression eval path.
  try {
    exec::ExprSetSimplified exprSetSimplified({plan}, execCtx_);
    exec::EvalCtx evalCtxSimplified(
        execCtx_, &exprSetSimplified, rowVector.get());

    try {
      exprSetSimplified.eval(rows, evalCtxSimplified, simplifiedEvalResult);
    } catch (...) {
      if (!canThrow) {
        LOG(ERROR)
            << "Simplified eval wasn't supposed to throw, but it did. Aborting.";
        throw;
      }
      exceptionSimplifiedPtr = std::current_exception();
    }
  } catch (...) {
    exceptionSimplifiedPtr = std::current_exception();
  }

  RowVectorPtr resultFromOperators;
  bool canExecuteViaOperators = false;
  if(rowVector->childrenSize() > 0 && hasWriterSupport(rowVector->type())){
    canExecuteViaOperators = true;
      try {
        resultFromOperators = evaluateUsingOperators(plan, rowVector);
      } catch (...) {
        if (!canThrow) {
          LOG(ERROR)
              << "Operator eval wasn't supposed to throw, but it did. Aborting.";
          throw;
        }
        exceptionOperatorPtr = std::current_exception();
      }
  }


  try {
    // Compare results or exceptions (if any). Fail is anything is different.
    if (exceptionCommonPtr || exceptionSimplifiedPtr || (canExecuteViaOperators && exceptionOperatorPtr)) {
      // Throws in case exceptions are not compatible. If they are compatible,
      // return false to signal that the expression failed.
      if(exceptionCommonPtr || exceptionSimplifiedPtr) compareExceptions(exceptionCommonPtr, exceptionSimplifiedPtr);
      if(canExecuteViaOperators && (exceptionCommonPtr || exceptionOperatorPtr)) compareExceptions(exceptionCommonPtr, exceptionOperatorPtr);
      return false;
    } else {
      // Throws in case output is different.
      compareVectors(commonEvalResult.front(), simplifiedEvalResult.front());
      if(canExecuteViaOperators) compareVectors(commonEvalResult.front(), resultFromOperators->childAt(0));
    }
  } catch (...) {
    if (!options_.reproPersistPath.empty() && !options_.persistAndRunOnce) {
      persistReproInfo(
          rowVector, columnsToWrapInLazy, copiedResult, sql, complexConstants);
    }
    throw;
  }

  if (!options_.reproPersistPath.empty() && options_.persistAndRunOnce) {
    // A guard to make sure it runs only once with persistAndRunOnce flag
    // turned on. It shouldn't reach here normally since the flag is used to
    // persist repro info for crash failures. But if it hasn't crashed by now,
    // we still don't want another iteration.
    LOG(WARNING)
        << "Iteration succeeded with --persist_and_run_once flag enabled "
           "(expecting crash failure)";
    exit(0);
  }

  return true;
}

void ExpressionVerifier::persistReproInfo(
    const VectorPtr& inputVector,
    std::vector<column_index_t> columnsToWrapInLazy,
    const VectorPtr& resultVector,
    const std::string& sql,
    const std::vector<VectorPtr>& complexConstants) {
  std::string inputPath;
  std::string lazyListPath;
  std::string resultPath;
  std::string sqlPath;
  std::string complexConstantsPath;

  const auto basePath = options_.reproPersistPath.c_str();
  if (!common::generateFileDirectory(basePath)) {
    return;
  }

  // Create a new directory
  auto dirPath = common::generateTempFolderPath(basePath, "expressionVerifier");
  if (!dirPath.has_value()) {
    LOG(INFO) << "Failed to create directory for persisting repro info.";
    return;
  }
  // Saving input vector
  inputPath = fmt::format("{}/{}", dirPath->c_str(), kInputVectorFileName);
  try {
    saveVectorToFile(inputVector.get(), inputPath.c_str());
  } catch (std::exception& e) {
    inputPath = e.what();
  }

  // Saving the list of column indices that are to be wrapped in lazy.
  if (!columnsToWrapInLazy.empty()) {
    lazyListPath =
        fmt::format("{}/{}", dirPath->c_str(), kIndicesOfLazyColumnsFileName);
    try {
      saveVectorTofile<column_index_t>(
          columnsToWrapInLazy, lazyListPath.c_str());
    } catch (std::exception& e) {
      lazyListPath = e.what();
    }
  }

  // Saving result vector
  if (resultVector) {
    resultPath = fmt::format("{}/{}", dirPath->c_str(), kResultVectorFileName);
    try {
      saveVectorToFile(resultVector.get(), resultPath.c_str());
    } catch (std::exception& e) {
      resultPath = e.what();
    }
  }

  // Saving sql
  sqlPath = fmt::format("{}/{}", dirPath->c_str(), kExpressionInSqlFileName);
  try {
    saveStringToFile(sql, sqlPath.c_str());
  } catch (std::exception& e) {
    sqlPath = e.what();
  }
  // Saving complex constants
  if (!complexConstants.empty()) {
    complexConstantsPath =
        fmt::format("{}/{}", dirPath->c_str(), kComplexConstantsFileName);
    try {
      saveVectorToFile(
          VectorMaker(complexConstants[0]->pool())
              .rowVector(complexConstants)
              .get(),
          complexConstantsPath.c_str());
    } catch (std::exception& e) {
      complexConstantsPath = e.what();
    }
  }

  std::stringstream ss;
  ss << "Persisted input: --input_path " << inputPath;
  if (resultVector) {
    ss << " --result_path " << resultPath;
  }
  ss << " --sql_path " << sqlPath;
  if (!columnsToWrapInLazy.empty()) {
    // TODO: add support for consuming this in ExpressionRunner
    ss << " --lazy_column_list_path " << lazyListPath;
  }
  if (!complexConstants.empty()) {
    ss << " --complex_constant_path " << complexConstantsPath;
  }
  LOG(INFO) << ss.str();
}

} // namespace facebook::velox::test
