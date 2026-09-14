#include <openssl/evp.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr std::size_t kPreambleSize = 32;
constexpr std::size_t kInlineSize = 48;
constexpr std::size_t kBlockSize = 16;
constexpr std::size_t kTagSize = 16;
constexpr std::uint8_t kLateComplete = 0x0e;

struct Segment {
  std::uint16_t alignment;
  std::vector<std::uint8_t> data;
};

using Bytes = std::vector<std::uint8_t>;

void put16(Bytes& output, std::size_t offset, std::uint16_t value) {
  output[offset] = static_cast<std::uint8_t>(value);
  output[offset + 1] = static_cast<std::uint8_t>(value >> 8);
}

void put32(Bytes& output, std::size_t offset, std::uint32_t value) {
  for (unsigned shift = 0; shift < 32; shift += 8) {
    output[offset + shift / 8] = static_cast<std::uint8_t>(value >> shift);
  }
}

void put64(Bytes& output, std::size_t offset, std::uint64_t value) {
  for (unsigned shift = 0; shift < 64; shift += 8) {
    output[offset + shift / 8] = static_cast<std::uint8_t>(value >> shift);
  }
}

std::uint32_t crc32c(std::uint32_t seed, std::span<const std::uint8_t> input) {
  std::uint32_t crc = seed;
  for (const std::uint8_t value : input) {
    crc ^= value;
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1) ^ (0x82f63b78U & mask);
    }
  }
  return crc;
}

Bytes preamble(std::uint8_t tag, const std::vector<Segment>& segments) {
  if (segments.empty() || segments.size() > 4) {
    throw std::runtime_error("invalid segment count");
  }
  Bytes output(kPreambleSize);
  output[0] = tag;
  output[1] = static_cast<std::uint8_t>(segments.size());
  for (std::size_t index = 0; index < segments.size(); ++index) {
    const std::size_t offset = 2 + index * 6;
    put32(output, offset, static_cast<std::uint32_t>(segments[index].data.size()));
    put16(output, offset + 4, segments[index].alignment);
  }
  put32(output, 28, crc32c(0, std::span(output).first(28)));
  return output;
}

Bytes crcFrame(std::uint8_t tag, const std::vector<Segment>& segments) {
  Bytes output = preamble(tag, segments);
  for (std::size_t index = 0; index < segments.size(); ++index) {
    output.insert(output.end(), segments[index].data.begin(), segments[index].data.end());
    if (index == 0 && !segments[index].data.empty()) {
      const std::size_t offset = output.size();
      output.resize(offset + 4);
      put32(output, offset, crc32c(UINT32_MAX, segments[index].data));
    }
  }
  if (segments.size() > 1) {
    output.push_back(kLateComplete);
    for (std::size_t index = 1; index < 4; ++index) {
      const std::uint32_t crc = index < segments.size()
                                    ? crc32c(UINT32_MAX, segments[index].data)
                                    : 0;
      const std::size_t offset = output.size();
      output.resize(offset + 4);
      put32(output, offset, crc);
    }
  }
  return output;
}

class GCM {
 public:
  explicit GCM(const std::array<std::uint8_t, 64>& secret)
      : key_(secret.begin(), secret.begin() + 16),
        nonce_(secret.begin() + 28, secret.begin() + 40) {}

  Bytes seal(std::span<const std::uint8_t> plaintext) {
    EVP_CIPHER_CTX* context = EVP_CIPHER_CTX_new();
    if (context == nullptr) {
      throw std::runtime_error("EVP_CIPHER_CTX_new failed");
    }
    Bytes output(plaintext.size() + kTagSize);
    int written = 0;
    int final = 0;
    const bool ok = EVP_EncryptInit_ex(context, EVP_aes_128_gcm(), nullptr,
                                       key_.data(), nonce_.data()) == 1 &&
                    EVP_EncryptUpdate(context, output.data(), &written,
                                      plaintext.data(), plaintext.size()) == 1 &&
                    EVP_EncryptFinal_ex(context, output.data() + written,
                                        &final) == 1 &&
                    EVP_CIPHER_CTX_ctrl(context, EVP_CTRL_GCM_GET_TAG, kTagSize,
                                        output.data() + plaintext.size()) == 1;
    EVP_CIPHER_CTX_free(context);
    if (!ok || static_cast<std::size_t>(written + final) != plaintext.size()) {
      throw std::runtime_error("AES-128-GCM encryption failed");
    }
    incrementNonce();
    return output;
  }

 private:
  void incrementNonce() {
    for (std::size_t index = 4; index < nonce_.size(); ++index) {
      if (++nonce_[index] != 0) {
        return;
      }
    }
    throw std::runtime_error("nonce counter exhausted");
  }

  Bytes key_;
  Bytes nonce_;
};

std::size_t padded(std::size_t size) {
  return (size + kBlockSize - 1) & ~(kBlockSize - 1);
}

Bytes secureFrame(std::uint8_t tag, const std::vector<Segment>& segments,
                  const std::array<std::uint8_t, 64>& secret) {
  GCM gcm(secret);
  Bytes first = preamble(tag, segments);
  first.resize(kPreambleSize + kInlineSize);
  const std::size_t inlineLength =
      std::min(segments[0].data.size(), kInlineSize);
  std::copy_n(segments[0].data.begin(), inlineLength,
              first.begin() + kPreambleSize);
  Bytes output = gcm.seal(first);

  const std::size_t firstPadded = padded(segments[0].data.size());
  if (firstPadded > kInlineSize) {
    Bytes remainder(firstPadded - kInlineSize);
    std::copy(segments[0].data.begin() + kInlineSize,
              segments[0].data.end(), remainder.begin());
    Bytes sealed = gcm.seal(remainder);
    output.insert(output.end(), sealed.begin(), sealed.end());
  }
  if (segments.size() == 1) {
    return output;
  }

  Bytes remaining;
  for (std::size_t index = 1; index < segments.size(); ++index) {
    const std::size_t offset = remaining.size();
    remaining.resize(offset + padded(segments[index].data.size()));
    std::copy(segments[index].data.begin(), segments[index].data.end(),
              remaining.begin() + offset);
  }
  remaining.resize(remaining.size() + kBlockSize);
  remaining[remaining.size() - kBlockSize] = kLateComplete;
  Bytes sealed = gcm.seal(remaining);
  output.insert(output.end(), sealed.begin(), sealed.end());
  return output;
}

void writeFile(const std::filesystem::path& directory, const std::string& name,
               const Bytes& data) {
  std::ofstream output(directory / name, std::ios::binary);
  output.write(reinterpret_cast<const char*>(data.data()), data.size());
  if (!output) {
    throw std::runtime_error("failed to write " + name);
  }
}

Bytes sequence(std::size_t size) {
  Bytes output(size);
  for (std::size_t index = 0; index < size; ++index) {
    output[index] = static_cast<std::uint8_t>(index);
  }
  return output;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) {
      std::fprintf(stderr, "usage: %s OUTPUT_DIRECTORY\n", argv[0]);
      return 2;
    }
    const std::filesystem::path outputDirectory(argv[1]);
    std::filesystem::create_directories(outputDirectory);

    Bytes banner{'c', 'e', 'p', 'h', ' ', 'v', '2', '\n', 16, 0};
    banner.resize(26);
    put64(banner, 10, 1);
    put64(banner, 18, 1);
    writeFile(outputDirectory, "banner-rev1.bin", banner);

    const std::vector<Segment> oneSegment{{8, {'c', 'e', 'p', 'h'}}};
    writeFile(outputDirectory, "crc-one-segment.bin", crcFrame(20, oneSegment));

    const std::vector<Segment> fourSegments{
        {8, {'h', 'e', 'a', 'd', 'e', 'r'}},
        {8, {}},
        {8, {'m', 'i', 'd', 'd', 'l', 'e'}},
        {4096, {0, 1, 2, 3}},
    };
    writeFile(outputDirectory, "crc-four-segment.bin",
              crcFrame(17, fourSegments));

    std::array<std::uint8_t, 64> secret{};
    for (std::size_t index = 0; index < secret.size(); ++index) {
      secret[index] = static_cast<std::uint8_t>(index);
    }
    writeFile(outputDirectory, "secure-one-segment.bin",
              secureFrame(20, oneSegment, secret));

    const std::vector<Segment> secureSegments{
        {8, sequence(63)},
        {8, {}},
        {8, {'m', 'i', 'd', 'd', 'l', 'e'}},
        {4096, sequence(32)},
    };
    writeFile(outputDirectory, "secure-multi-record.bin",
              secureFrame(17, secureSegments, secret));
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "p02-oracle: %s\n", error.what());
    return 1;
  }
}