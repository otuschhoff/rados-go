#include <arpa/inet.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#if !defined(__linux__)
#error "this fixture generator requires the Linux socket ABI"
#endif

_Static_assert(AF_INET6 == 10, "unexpected Linux AF_INET6 value");
_Static_assert(sizeof(struct sockaddr_in6) == 28, "unexpected sockaddr_in6 size");
_Static_assert(offsetof(struct sockaddr_in6, sin6_family) == 0, "unexpected family offset");
_Static_assert(offsetof(struct sockaddr_in6, sin6_port) == 2, "unexpected port offset");
_Static_assert(offsetof(struct sockaddr_in6, sin6_flowinfo) == 4, "unexpected flowinfo offset");
_Static_assert(offsetof(struct sockaddr_in6, sin6_addr) == 8, "unexpected address offset");
_Static_assert(offsetof(struct sockaddr_in6, sin6_scope_id) == 24, "unexpected scope offset");

static int write_u32_le(uint32_t value) {
  const unsigned char bytes[] = {
      (unsigned char)value,
      (unsigned char)(value >> 8),
      (unsigned char)(value >> 16),
      (unsigned char)(value >> 24),
  };
  return fwrite(bytes, sizeof(bytes), 1, stdout) == 1 ? 0 : 1;
}

int main(void) {
  struct sockaddr_in6 address;
  memset(&address, 0, sizeof(address));
  address.sin6_family = AF_INET6;
  address.sin6_port = htons(3300);
  address.sin6_flowinfo = UINT32_C(0x01020304);
  address.sin6_scope_id = UINT32_C(0x05060708);
  if (inet_pton(AF_INET6, "2001:db8::1234", &address.sin6_addr) != 1) {
    return 1;
  }
  if (fputc(1, stdout) == EOF || fputc(1, stdout) == EOF ||
      fputc(1, stdout) == EOF || write_u32_le(40) || write_u32_le(2) ||
      write_u32_le(7) || write_u32_le(sizeof(address)) ||
      fwrite(&address, sizeof(address), 1, stdout) != 1) {
    return 1;
  }
  return 0;
}
