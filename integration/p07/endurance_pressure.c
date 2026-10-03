#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/inotify.h>
#include <sys/signalfd.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

static int setting(const char *root, const char *name, unsigned long long value) {
    char file[1024]; snprintf(file, sizeof(file), "%s/%s", root, name);
    FILE *output = fopen(file, "w"); if (!output) return -1;
    int failed = fprintf(output, "%llu\n", value) < 0;
    if (fclose(output)) failed = 1;
    return failed ? -1 : 0;
}

int main(int argc, char **argv) {
    if (argc < 5 || strncmp(argv[1], "/sys/fs/cgroup/p11-", 19) || strlen(argv[1]) > 900) return 2;
    char file[1024]; snprintf(file, sizeof(file), "%s/cgroup.procs", argv[1]);
    FILE *group = fopen(file, "w"); if (!group) return 1;
    if (fprintf(group, "%ld\n", (long)getpid()) < 0 || fclose(group)) return 1;
    int notify = inotify_init1(IN_CLOEXEC); if (notify < 0) return 1;
    if (inotify_add_watch(notify, argv[2], IN_CLOSE_WRITE) < 0) return 1;
    sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGCHLD);
    if (sigprocmask(SIG_BLOCK, &mask, NULL)) return 1;
    int child_signal = signalfd(-1, &mask, SFD_CLOEXEC); if (child_signal < 0) return 1;
    pid_t child = fork(); if (child < 0) return 1;
    if (!child) { if (prctl(PR_SET_PDEATHSIG, SIGTERM) || getppid() == 1) _exit(127); sigprocmask(SIG_UNBLOCK, &mask, NULL); execvp(argv[3], argv + 3); _exit(127); }
    volatile unsigned char *pressure = NULL;
    char events[4096];
    int ready = 0;
    while (!ready) {
        struct pollfd descriptors[2] = {{.fd = notify, .events = POLLIN}, {.fd = child_signal, .events = POLLIN}};
        if (poll(descriptors, 2, -1) < 0) { if (errno == EINTR) continue; kill(child, SIGTERM); waitpid(child, NULL, 0); return 1; }
        if (descriptors[1].revents) { waitpid(child, NULL, 0); return 1; }
        ssize_t count = read(notify, events, sizeof(events));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { kill(child, SIGTERM); waitpid(child, NULL, 0); return 1; }
        for (char *cursor = events; cursor < events + count;) {
            struct inotify_event *event = (struct inotify_event *)cursor;
            if (event->len && !strcmp(event->name, "window2.capture.json")) ready = 1;
            cursor += sizeof(*event) + event->len;
        }
    }
    close(notify);
    close(child_signal);
    snprintf(file, sizeof(file), "%s/memory.peak", argv[1]);
    FILE *input = fopen(file, "r"); unsigned long long baseline = 0;
    if (!input || fscanf(input, "%llu", &baseline) != 1) { if (input) fclose(input); kill(child, SIGTERM); waitpid(child, NULL, 0); return 1; }
    fclose(input);
    snprintf(file, sizeof(file), "%s/memory.current", argv[1]);
    input = fopen(file, "r"); unsigned long long current = 0;
    if (!input || fscanf(input, "%llu", &current) != 1) { if (input) fclose(input); kill(child, SIGTERM); waitpid(child, NULL, 0); return 1; }
    fclose(input);
    const size_t bytes = 256ULL << 20;
    if (baseline > (512ULL << 20) || setting(argv[1], "memory.max", baseline + (512ULL << 20)) || setting(argv[1], "memory.high", baseline + (248ULL << 20))) {
        kill(child, SIGTERM); waitpid(child, NULL, 0); return 1;
    }
    pressure = malloc(bytes); if (!pressure) { kill(child, SIGTERM); waitpid(child, NULL, 0); return 1; }
    for (size_t offset = 0; offset < bytes; offset += 4096) pressure[offset] = (unsigned char)(offset / 4096);
    fprintf(stderr, "{\"helper_bytes\":%zu,\"baseline_source\":\"memory.peak_after_full_cycle\",\"initial_group_bytes\":%llu,\"initial_group_current_bytes\":%llu,\"high_bytes\":%llu,\"max_bytes\":%llu}\n", bytes, baseline, current, baseline + (248ULL << 20), baseline + (512ULL << 20));
    int status = 0; while (waitpid(child, &status, 0) < 0) if (errno != EINTR) { free((void *)pressure); return 1; }
    free((void *)pressure);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}