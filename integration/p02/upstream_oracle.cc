#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "msg/async/compression_onwire.h"
#include "msg/async/frames_v2.h"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

using ceph::bufferlist;
using compression_rxtx_t = ceph::compression::onwire::rxtx_t;
using crypto_rxtx_t = ceph::crypto::onwire::rxtx_t;
using ceph::msgr::v2::AckFrame;
using ceph::msgr::v2::FrameAssembler;
using ceph::msgr::v2::MessageFrame;
using ceph::msgr::v2::Tag;
using ceph::msgr::v2::segment_t;

namespace {

void write_vector(const std::filesystem::path& directory, const char* name,
                  bufferlist& frame) {
  const auto path = directory / name;
  if (frame.write_file(path.c_str(), 0644) != 0) {
    throw std::runtime_error("failed to write " + path.string());
  }
}

bufferlist make_sequence(std::size_t size) {
  std::string data(size, '\0');
  for (std::size_t index = 0; index < size; ++index) {
    data[index] = static_cast<char>(index);
  }
  bufferlist output;
  output.append(data);
  return output;
}

void emit_vectors(const std::filesystem::path& directory) {
  std::filesystem::create_directories(directory);
  crypto_rxtx_t no_crypto;
  compression_rxtx_t no_compression;

  const std::uint16_t one_aligns[] = {segment_t::DEFAULT_ALIGNMENT};
  bufferlist one_segments[1];
  one_segments[0].append("ceph", 4);
  FrameAssembler crc_enabled(&no_crypto, true, true, &no_compression);
  auto crc_one = crc_enabled.assemble_frame(Tag::ACK, one_segments,
                                             one_aligns, 1);
  write_vector(directory, "upstream-crc-one-segment.bin", crc_one);

  bufferlist disabled_segments[1];
  disabled_segments[0].append("ceph", 4);
  FrameAssembler crc_disabled(&no_crypto, true, false, &no_compression);
  auto crc_disabled_one = crc_disabled.assemble_frame(
      Tag::ACK, disabled_segments, one_aligns, 1);
  write_vector(directory, "upstream-crc-disabled-one-segment.bin",
               crc_disabled_one);

  const std::uint16_t four_aligns[] = {
      segment_t::DEFAULT_ALIGNMENT, segment_t::DEFAULT_ALIGNMENT,
      segment_t::DEFAULT_ALIGNMENT, segment_t::PAGE_SIZE_ALIGNMENT};
  bufferlist four_segments[4];
  four_segments[0].append("header", 6);
  four_segments[2].append("middle", 6);
  const char data[] = {0, 1, 2, 3};
  four_segments[3].append(data, sizeof(data));
  auto crc_four = crc_enabled.assemble_frame(Tag::MESSAGE, four_segments,
                                              four_aligns, 4);
  write_vector(directory, "upstream-crc-four-segment.bin", crc_four);

  auto ack = AckFrame::Encode(0x0102030405060708ULL);
  auto ack_frame = ack.get_buffer(crc_enabled);
  write_vector(directory, "upstream-ack-control.bin", ack_frame);

  ceph_msg_header2 header{};
  header.seq = 0x0102030405060708ULL;
  header.tid = 0x1112131415161718ULL;
  header.type = 0x2122;
  header.priority = 0x3132;
  header.version = 0x4142;
  header.data_pre_padding_len = 2;
  header.data_off = 0x5152;
  header.ack_seq = 0x6162636465666768ULL;
  header.flags = 0x71;
  header.compat_version = 0x8182;
  header.reserved = 0;
  bufferlist front;
  front.append("front", 5);
  bufferlist middle;
  middle.append("mid", 3);
  bufferlist message_data;
  const char message_bytes[] = {0, 1};
  message_data.append(message_bytes, sizeof(message_bytes));
  auto message = MessageFrame::Encode(header, front, middle, message_data);
  auto message_frame = message.get_buffer(crc_enabled);
  write_vector(directory, "upstream-message-frame.bin", message_frame);

  AuthConnectionMeta auth_meta;
  auth_meta.con_mode = CEPH_CON_MODE_SECURE;
  auth_meta.connection_secret.resize(64);
  for (std::size_t index = 0; index < auth_meta.connection_secret.size();
       ++index) {
    auth_meta.connection_secret[index] = static_cast<char>(index);
  }
  auto crypto = crypto_rxtx_t::create_handler_pair(
      g_ceph_context, auth_meta, true, false);
  FrameAssembler secure(&crypto, true, true, &no_compression);

  bufferlist secure_one_segments[1];
  secure_one_segments[0].append("ceph", 4);
  auto secure_one = secure.assemble_frame(Tag::ACK, secure_one_segments,
                                           one_aligns, 1);
  write_vector(directory, "upstream-secure-one-segment.bin", secure_one);

  bufferlist secure_four_segments[4];
  secure_four_segments[0] = make_sequence(63);
  secure_four_segments[2].append("middle", 6);
  secure_four_segments[3] = make_sequence(32);
  auto secure_four = secure.assemble_frame(Tag::MESSAGE, secure_four_segments,
                                            four_aligns, 4);
  write_vector(directory, "upstream-secure-multi-record.bin", secure_four);
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 2) {
    return 2;
  }
  auto args = argv_to_vec(argc, argv);
  auto context = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY,
                             CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(context.get());
  emit_vectors(argv[1]);
  return 0;
}
