#define _GNU_SOURCE

#include <dlfcn.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <time.h>

static int (*original)(int, int, int, void *, socklen_t *);

/* Resolve before worker threads start. */
__attribute__((constructor)) static void resolve_getsockopt(void) {
    original = dlsym(RTLD_NEXT, "getsockopt");
}

/* Delay only the reporter's TCP_INFO query while receive workers keep running. */
int getsockopt(int descriptor, int level, int option, void *value, socklen_t *length) {
    if (level == IPPROTO_TCP && option == TCP_INFO) {
        const char *configured = getenv("WIREHOP_TEST_STATS_DELAY_NS");
        if (configured) {
            long delay = strtol(configured, NULL, 10);
            struct timespec pause = {delay / 1000000000, delay % 1000000000};
            nanosleep(&pause, NULL);
        }
    }
    return original(descriptor, level, option, value, length);
}
