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
#pragma once

#include "velox/functions/Udf.h"
#include "velox/type/Conversions.h"

namespace facebook::velox::functions {

template <typename TExecCtx, bool isMax>
struct ArrayMinMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  template <typename T>
  void update(T& currentValue, const T& candidateValue) {
    if constexpr (isMax) {
      if (candidateValue > currentValue) {
        currentValue = candidateValue;
      }
    } else {
      if (candidateValue < currentValue) {
        currentValue = candidateValue;
      }
    }
  }

  template <typename TReturn, typename TInput>
  void assign(TReturn& out, const TInput& value) {
    out = value;
  }
// This is a specialization! why only for this? why not other complex types like an array, we can have an array of arrays?? right?
  void assign(out_type<Varchar>& out, const arg_type<Varchar>& value) {
    // TODO: reuse strings once support landed.
    out.resize(value.size());
    if (value.size() != 0) {
      std::memcpy(out.data(), value.data(), value.size());
    }
  }

  template <typename TReturn, typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TReturn& out, const TInput& array) {
    // Result is null if array is empty.
    if (array.size() == 0) {
      return false;
    }

    if (!array.mayHaveNulls()) {
      // Input array does not have nulls.
      auto currentValue = *array[0];
      for (auto i = 1; i < array.size(); i++) {
        update(currentValue, array[i].value());
      }
      assign(out, currentValue);
      return true;
    }

    auto it = array.begin();
    // Result is null if any element is null.
    if (!it->has_value()) {
      return false;
    }

    auto currentValue = it->value();
    ++it;
    while (it != array.end()) {
      if (!it->has_value()) {
        return false;
      }
      update(currentValue, it->value());
      ++it;
    }

    assign(out, currentValue);
    return true;
  }
};

template <typename TExecCtx>
struct ArrayMinFunction : public ArrayMinMaxFunction<TExecCtx, false> {};

template <typename TExecCtx>
struct ArrayMaxFunction : public ArrayMinMaxFunction<TExecCtx, true> {};

template <typename TExecCtx, typename T>
struct ArrayJoinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  template <typename C>
  void writeValue(out_type<velox::Varchar>& result, const C& value) {
    bool nullOutput = false;
    result +=
        util::Converter<CppToType<velox::Varchar>::typeKind, void, false>::cast(
            value, nullOutput);
  }

  template <typename C>
  void writeOutput(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varchar>& delim,
      const C& value,
      bool& firstNonNull) {
    if (!firstNonNull) {
      writeValue(result, delim);
    }
    writeValue(result, value);
    firstNonNull = false;
  }

  void createOutputString(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim,
      std::optional<std::string> nullReplacement = std::nullopt) {
    bool firstNonNull = true;
    if (inputArray.size() == 0) {
      return;
    }

    for (const auto& entry : inputArray) {
      if (entry.has_value()) {
        writeOutput(result, delim, entry.value(), firstNonNull);
      } else if (nullReplacement.has_value()) {
        writeOutput(result, delim, nullReplacement.value(), firstNonNull);
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim) {
    createOutputString(result, inputArray, delim);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim,
      const arg_type<velox::Varchar>& nullReplacement) {
    createOutputString(result, inputArray, delim, nullReplacement.getString());
    return true;
  }
};

template <typename TExecParams, typename T>
struct CombinationsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  static const int64_t MAX_COMBINATION_LENGTH = 5;
  static const int64_t MAX_RESULT_ELEMENTS = 100000;

  /*static constexpr int32_t reuse_strings_from_arg =
      std::is_same<T, Varchar>::value ? 0 : -1;*/

  int64_t CalculateNumOfCombinations(
      int64_t numElements,
      int64_t combinationLength) {
    int64_t numCombos = 1;
    for (int i = 1; i <= combinationLength; i++) {
      numCombos = (numCombos * (numElements - combinationLength + i)) / i;
    }
    return numCombos;
  }

  void resetCombination(std::vector<int>& combination, int to) {
    for (int i = 0; i < to; i++) {
      combination[i] = i;
    }
  }

  std::vector<int> firstCombination(int64_t size) {
    std::vector<int> combination(size, 0);
    std::iota(combination.begin(), combination.end(), 0);
    return combination;
  }

  bool nextCombination(std::vector<int>& combination, int64_t inputArraySize) {
    for (int i = 0; i < combination.size() - 1; i++) {
      if (combination[i] + 1 < combination[i + 1]) {
        combination[i]++;
        resetCombination(combination, i);
        return true;
      }
    }
    if (combination.size() > 0 && combination.back() + 1 < inputArraySize) {
      combination.back()++;
      resetCombination(combination, combination.size() - 1);
      return true;
    }
    return false;
  }

  void appendEntryFromCombination(
      out_type<velox::Array<velox::Array<T>>>& result,
      const arg_type<velox::Array<T>>& array,
      std::vector<int>& combination) {
    auto& innerArray = result.add_item();
    for (int idx : combination) {
      if constexpr (std::is_same<T, Varchar>::value) {
        innerArray.add_item().setNoCopy(array[idx].value());
      } else {
        innerArray.add_item() = array[idx].value();
      }
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Array<velox::Array<T>>>& result,
      const arg_type<velox::Array<T>>& array,
      const int64_t& combinationLength) {
    auto arraySize = array.size();
    if(combinationLength > arraySize) return; // An empty array should be returned
    if (combinationLength > MAX_COMBINATION_LENGTH || combinationLength < 0)
      return; // TODO: throw appropriate error here
    int64_t numCombinations =
        CalculateNumOfCombinations(arraySize, combinationLength);
    if (numCombinations > MAX_RESULT_ELEMENTS)
      return; // TODO: throw appropriate error here

    result.reserve(numCombinations);
    std::vector<int> currCombination = firstCombination(combinationLength);
    do {
      appendEntryFromCombination(result, array, currCombination);
    } while (nextCombination(currCombination, arraySize));
  }
};

} // namespace facebook::velox::functions
