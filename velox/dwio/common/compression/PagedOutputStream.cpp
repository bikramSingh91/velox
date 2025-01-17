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

#include "velox/dwio/common/compression/PagedOutputStream.h"

namespace facebook::velox::dwio::common::compression {

std::vector<folly::StringPiece> PagedOutputStream::createPage() {
  auto origSize = buffer_.size();
  DWIO_ENSURE_GT(origSize, pageHeaderSize_);
  origSize -= pageHeaderSize_;

  auto compressedSize = origSize;
  // apply compressoin if there is compressor and original data size exceeds
  // threshold
  if (compressor_ && origSize >= threshold_) {
    compressionBuffer_ = pool_.getBuffer(buffer_.size());
    compressedSize = compressor_->compress(
        buffer_.data() + pageHeaderSize_,
        compressionBuffer_->data() + pageHeaderSize_,
        origSize);
  }

  folly::StringPiece compressed;
  if (compressedSize >= origSize) {
    // write orig
    writeHeader(buffer_.data(), origSize, true);
    compressed = folly::StringPiece(buffer_.data(), origSize + pageHeaderSize_);
  } else {
    // write compressed
    writeHeader(compressionBuffer_->data(), compressedSize, false);
    compressed = folly::StringPiece(
        compressionBuffer_->data(), compressedSize + pageHeaderSize_);
  }

  if (!encrypter_) {
    return {compressed};
  }

  encryptionBuffer_ = encrypter_->encrypt(folly::StringPiece(
      compressed.begin() + pageHeaderSize_, compressed.end()));
  updateSize(
      const_cast<char*>(compressed.begin()), encryptionBuffer_->length());
  return {
      folly::StringPiece(compressed.begin(), pageHeaderSize_),
      folly::StringPiece(
          reinterpret_cast<const char*>(encryptionBuffer_->data()),
          encryptionBuffer_->length())};
}

void PagedOutputStream::writeHeader(
    char* buffer,
    size_t compressedSize,
    bool original) {
  DWIO_ENSURE_LT(compressedSize, 1 << 23);
  buffer[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
  buffer[1] = static_cast<char>(compressedSize >> 7);
  buffer[2] = static_cast<char>(compressedSize >> 15);
}

void PagedOutputStream::updateSize(char* buffer, size_t compressedSize) {
  DWIO_ENSURE_LT(compressedSize, 1 << 23);
  buffer[0] = ((buffer[0] & 0x01) | static_cast<char>(compressedSize << 1));
  buffer[1] = static_cast<char>(compressedSize >> 7);
  buffer[2] = static_cast<char>(compressedSize >> 15);
}

void PagedOutputStream::resetBuffers() {
  // reset compression buffer size and return
  if (compressionBuffer_) {
    pool_.returnBuffer(std::move(compressionBuffer_));
  }
  encryptionBuffer_ = nullptr;
}

uint64_t PagedOutputStream::flush() {
  auto size = buffer_.size();
  auto originalSize = bufferHolder_.size();
  if (size > pageHeaderSize_) {
    bufferHolder_.take(createPage());
    resetBuffers();
    // reset input buffer
    buffer_.resize(pageHeaderSize_);
  }
  return bufferHolder_.size() - originalSize;
}

void PagedOutputStream::BackUp(int32_t count) {
  if (count > 0) {
    DWIO_ENSURE_GE(buffer_.size(), count + pageHeaderSize_);
    BufferedOutputStream::BackUp(count);
  }
}

bool PagedOutputStream::Next(void** data, int32_t* size, uint64_t increment) {
  if (!tryResize(data, size, pageHeaderSize_, increment)) {
    flushAndReset(data, size, pageHeaderSize_, createPage());
    resetBuffers();
  }
  return true;
}

void PagedOutputStream::recordPosition(
    PositionRecorder& recorder,
    int32_t bufferLength,
    int32_t bufferOffset,
    int32_t strideOffset) const {
  // add compressed size, then uncompressed
  recorder.add(bufferHolder_.size(), strideOffset);
  auto size = buffer_.size();
  if (size) {
    size -= (pageHeaderSize_ + bufferLength - bufferOffset);
  }
  recorder.add(size, strideOffset);
}

} // namespace facebook::velox::dwio::common::compression
