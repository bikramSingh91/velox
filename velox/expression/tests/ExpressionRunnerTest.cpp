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

#include "velox/expression/tests/ExpressionRunner.h"
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/expression/Expr.h"

using namespace facebook::velox;
using facebook::velox::exec::test::PrestoQueryRunner;
using facebook::velox::test::ReferenceQueryRunner;


DEFINE_string(
    reference_db_url,
    "http://127.0.0.1:8080",
    "ReferenceDB URI along with port. If set, we use the reference DB as the "
    "source of truth. Otherwise, use Velox simplified eval path. Example: "
    "--reference_db_url=http://127.0.0.1:8080");

DEFINE_uint32(
    req_timeout_ms,
    10000,
    "Timeout in milliseconds for HTTP requests made to reference DB, "
    "such as Presto. Example: --req_timeout_ms=2000");

DEFINE_string(json_queries_path, "", "");

class JsonExtractTestSuite : public facebook::velox::test::VectorTestBase {

  std::vector<core::TypedExprPtr> parseSql(
      const std::string& sql,
      const TypePtr& inputType,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants) {
    auto exprs = parse::parseMultipleExpressions(sql, {});

    std::vector<core::TypedExprPtr> typedExprs;
    typedExprs.reserve(exprs.size());
    for (const auto& expr : exprs) {
      typedExprs.push_back(
          core::Expressions::inferTypes(expr, inputType, pool, complexConstants));
    }
    return typedExprs;
  }

  RowVectorPtr createRowVector(
      const std::vector<VectorPtr>& vectors,
      vector_size_t size,
      memory::MemoryPool* pool) {
    auto n = vectors.size();

    std::vector<std::string> names;
    names.reserve(n);
    std::vector<TypePtr> types;
    types.reserve(n);
    for (auto i = 0; i < n; ++i) {
      names.push_back(fmt::format("_col{}", i));
      types.push_back(vectors[i]->type());
    }

    return std::make_shared<RowVector>(
        pool, ROW(std::move(names), std::move(types)), nullptr, size, vectors);
  }

  RowVectorPtr evaluateAndPrintResults(
      exec::ExprSet& exprSet,
      const RowVectorPtr& data,
      const SelectivityVector& rows,
      core::ExecCtx& execCtx) {
    exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

    std::vector<VectorPtr> results(1);
    exprSet.eval(rows, evalCtx, results);

    // Print the results.
    auto rowResult = createRowVector(results, rows.size(), execCtx.pool());
    std::cout << "Result: " << rowResult->type()->toString() << std::endl;
    exec::test::printResults(rowResult, std::cout);
    return rowResult;
  }

 public:
  void execute(
      std::string rootFolderPath,
      std::shared_ptr<ReferenceQueryRunner>& referenceQueryRunner) {
    // Specify the file name to read from each subfolder
    std::string jsonfileName = "document.json";
    std::string selectorFileName = "selector";
    // Iterate through all subfolders in the root folder
    std::vector<std::string> testNames;
    std::vector<std::string> jsons;
    std::vector<std::string> jsonPaths;
    for (const auto& entry :
         std::filesystem::directory_iterator(rootFolderPath)) {
      if (entry.is_directory()) {
        testNames.push_back(entry.path().stem().string());

        std::string filePath = entry.path().string() + "/" + jsonfileName;
        jsons.push_back(restoreStringFromFile(filePath.c_str()));

        filePath = entry.path().string() + "/" + selectorFileName;
        auto str = restoreStringFromFile(filePath.c_str());
        if(!str.empty() && str.back() == '\n') str.pop_back();
        jsonPaths.push_back(str);
      }
    }

    auto inputVector = makeRowVector(
        {"test_name", "json_col", "path_col"},
        {makeFlatVector<std::string>(testNames),
         makeFlatVector<std::string>(jsons),
         makeFlatVector<std::string>(jsonPaths)});
    SelectivityVector rows(inputVector->size());
    std::string sql = "test_name,path_col, try(json_extract(json_parse(json_col), path_col))";
    VectorPtr complexConstants{nullptr};
    parse::registerTypeResolver();
    auto typedExprs =
        parseSql(sql, inputVector->type(), pool(), complexConstants);
    VectorPtr resultVector = BaseVector::create(
        ROW({"c0", "c1"}, {VARCHAR(), VARCHAR()}), inputVector->size(), pool());
    auto queryCtx = core::QueryCtx::create();
    core::ExecCtx execCtx{pool(), queryCtx.get()};
    auto verifier =
        test::ExpressionVerifier(&execCtx, {false, ""}, referenceQueryRunner);
    verifier.verify(
        typedExprs,
        inputVector,
        std::nullopt,
        std::move(resultVector),
        true,
        {});

    //exec::ExprSet exprSet(typedExprs, &execCtx);
    //auto results = evaluateAndPrintResults(exprSet, inputVector, rows, execCtx);
  }
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  VELOX_CHECK(!FLAGS_json_queries_path.empty());

  memory::initializeMemoryManager({});

  filesystems::registerLocalFileSystem();
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  exec::test::registerHiveConnector({});
  dwrf::registerDwrfWriterFactory();

  functions::prestosql::registerAllScalarFunctions();

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  if (!FLAGS_reference_db_url.empty()) {
    referenceQueryRunner = std::make_shared<PrestoQueryRunner>(
        rootPool.get(),
        FLAGS_reference_db_url,
        "expression_runner_test",
        static_cast<std::chrono::milliseconds>(FLAGS_req_timeout_ms));
    LOG(INFO) << "Using Presto as the reference DB.";
  }

  // Specify the root folder path
  std::string rootFolderPath = FLAGS_json_queries_path;
  JsonExtractTestSuite testSuite;
  testSuite.execute(rootFolderPath, referenceQueryRunner);
}
