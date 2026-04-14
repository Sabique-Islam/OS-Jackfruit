/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

// Supervised Multi-Container Runtime (User Space)

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 32
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_HARD_LIMIT_KILLED,
    CONTAINER_EXITED,
    CONTAINER_KILLED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char final_reason[64];
    char log_path[PATH_MAX];
    pthread_cond_t exited_cond;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile sig_atomic_t should_stop;
    pthread_t logger_thread;
    pthread_t reaper_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} request_context_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} reader_context_t;

static volatile sig_atomic_t g_shutdown_requested = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_EXITED:
        return "exited";
    case CONTAINER_KILLED:
        return "killed";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    int rc = 0;

    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        rc = -1;
        goto out;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);

out:
    pthread_mutex_unlock(&buffer->mutex);
    return rc;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    int rc = 0;

    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        rc = -1;
        goto out;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);

out:
    pthread_mutex_unlock(&buffer->mutex);
    return rc;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *record;

    for (record = ctx->containers; record != NULL; record = record->next) {
        if (strncmp(record->id, id, CONTAINER_ID_LEN) == 0)
            return record;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *record;

    for (record = ctx->containers; record != NULL; record = record->next) {
        if (record->host_pid == pid)
            return record;
    }

    return NULL;
}

static int append_text(char *buffer, size_t buffer_len, size_t *offset, const char *fmt, ...)
{
    va_list args;
    int written;

    if (*offset >= buffer_len)
        return -1;

    va_start(args, fmt);
    written = vsnprintf(buffer + *offset, buffer_len - *offset, fmt, args);
    va_end(args);

    if (written < 0)
        return -1;

    if ((size_t)written >= buffer_len - *offset) {
        *offset = buffer_len - 1;
        buffer[*offset] = '\0';
        return -1;
    }

    *offset += (size_t)written;
    return 0;
}

static int send_response_text(int fd, int status, const char *text)
{
    control_response_t response;
    size_t len = 0;

    memset(&response, 0, sizeof(response));
    response.status = status;

    if (text) {
        len = strnlen(text, sizeof(response.message) - 1);
        memcpy(response.message, text, len);
        response.message[len] = '\0';
    }

    return (write(fd, &response, sizeof(response)) == (ssize_t)sizeof(response)) ? 0 : -1;
}

static int read_full(int fd, void *buffer, size_t length)
{
    size_t total = 0;

    while (total < length) {
        ssize_t rc = read(fd, (char *)buffer + total, length - total);

        if (rc == 0)
            return -1;
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        total += (size_t)rc;
    }

    return 0;
}

static int write_full(int fd, const void *buffer, size_t length)
{
    size_t total = 0;

    while (total < length) {
        ssize_t rc = write(fd, (const char *)buffer + total, length - total);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        total += (size_t)rc;
    }

    return 0;
}

static int ensure_directory(const char *path)
{
    if (mkdir(path, 0755) < 0 && errno != EEXIST)
        return -1;
    return 0;
}

static void copy_cstr(char *dst, size_t dst_size, const char *src)
{
    if (dst_size == 0)
        return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    snprintf(dst, dst_size, "%s", src);
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char log_path[PATH_MAX];
        FILE *log_file;

        memset(log_path, 0, sizeof(log_path));

        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *record = find_container_locked(ctx, item.container_id);

            if (record)
                copy_cstr(log_path, sizeof(log_path), record->log_path);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0')
            continue;

        log_file = fopen(log_path, "ab");
        if (!log_file)
            continue;

        fwrite(item.data, 1, item.length, log_file);
        fflush(log_file);
        fclose(log_file);
    }

    return NULL;
}

static void *reader_thread(void *arg)
{
    reader_context_t *reader = arg;
    char buffer[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t bytes_read = read(reader->read_fd, buffer, sizeof(buffer));

        if (bytes_read == 0)
            break;

        if (bytes_read < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        log_item_t item;

        memset(&item, 0, sizeof(item));
        copy_cstr(item.container_id, sizeof(item.container_id), reader->container_id);
        item.length = (size_t)bytes_read;
        memcpy(item.data, buffer, (size_t)bytes_read);

        if (bounded_buffer_push(&reader->ctx->log_buffer, &item) != 0)
            break;
    }

    close(reader->read_fd);
    free(reader);
    return NULL;
}

static void update_terminal_state_locked(container_record_t *record, int status)
{
    if (WIFEXITED(status)) {
        record->state = CONTAINER_EXITED;
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        snprintf(record->final_reason, sizeof(record->final_reason), "normal_exit");
        return;
    }

    if (WIFSIGNALED(status)) {
        record->exit_signal = WTERMSIG(status);

        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            snprintf(record->final_reason, sizeof(record->final_reason), "stopped");
            return;
        }

        if (record->exit_signal == SIGKILL) {
            record->state = CONTAINER_HARD_LIMIT_KILLED;
            snprintf(record->final_reason, sizeof(record->final_reason), "hard_limit_killed");
            return;
        }

        record->state = CONTAINER_KILLED;
        snprintf(record->final_reason, sizeof(record->final_reason), "signal_%d", record->exit_signal);
        return;
    }

    record->state = CONTAINER_KILLED;
    snprintf(record->final_reason, sizeof(record->final_reason), "unknown_exit");
}

static void *reaper_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;

    for (;;) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, 0);

        if (pid < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ECHILD)
                break;
            continue;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *record = find_container_by_pid_locked(ctx, pid);

            if (record) {
                update_terminal_state_locked(record, status);
                unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid);
                pthread_cond_broadcast(&record->exited_cond);
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    if (cfg->id[0] != '\0') {
        char hostname[64];

        snprintf(hostname, sizeof(hostname), "%s", cfg->id);
        if (sethostname(hostname, strnlen(hostname, sizeof(hostname))) < 0)
            perror("sethostname");
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount private");

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        _exit(1);
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        _exit(1);
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST)
        perror("mkdir /proc");

    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    if (cfg->nice_value != 0)
        setpriority(PRIO_PROCESS, 0, cfg->nice_value);

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        _exit(1);
    }

    close(cfg->log_write_fd);

    execl("/bin/sh", "sh", "-lc", cfg->command, (char *)NULL);
    perror("execl");
    _exit(127);
}

static int launch_container(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            container_record_t **out_record)
{
    container_record_t *record = NULL;
    child_config_t *child_cfg = NULL;
    char *stack = NULL;
    int pipefd[2] = { -1, -1 };
    pid_t child_pid;
    int rc = -1;

    if (req->container_id[0] == '\0' || req->rootfs[0] == '\0' || req->command[0] == '\0')
        return -1;

    if (access(req->rootfs, F_OK) != 0)
        return -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        errno = EEXIST;
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) < 0)
        return -1;

    record = calloc(1, sizeof(*record));
    if (!record)
        goto out;

    child_cfg = calloc(1, sizeof(*child_cfg));
    if (!child_cfg)
        goto out;

    stack = malloc(STACK_SIZE);
    if (!stack)
        goto out;

    copy_cstr(record->id, sizeof(record->id), req->container_id);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    record->stop_requested = 0;
    snprintf(record->final_reason, sizeof(record->final_reason), "starting");
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    pthread_cond_init(&record->exited_cond, NULL);

    copy_cstr(child_cfg->id, sizeof(child_cfg->id), req->container_id);
    copy_cstr(child_cfg->rootfs, sizeof(child_cfg->rootfs), req->rootfs);
    copy_cstr(child_cfg->command, sizeof(child_cfg->command), req->command);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    child_pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWPID | SIGCHLD,
                      child_cfg);
    if (child_pid < 0)
        goto out;

    close(pipefd[1]);
    pipefd[1] = -1;

    record->host_pid = child_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;

    if (register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) < 0) {
        fprintf(stderr,
                "warning: failed to register container %s with /dev/container_monitor: %s\n",
                record->id,
                strerror(errno));
    }

    reader_context_t *reader = calloc(1, sizeof(*reader));
    if (reader) {
        pthread_t thread;

        reader->ctx = ctx;
        reader->read_fd = pipefd[0];
        pipefd[0] = -1;
        copy_cstr(reader->container_id, sizeof(reader->container_id), record->id);
        if (pthread_create(&thread, NULL, reader_thread, reader) == 0)
            pthread_detach(thread);
        else {
            close(reader->read_fd);
            free(reader);
        }
    }

    if (out_record)
        *out_record = record;

    rc = 0;

out:
    if (pipefd[0] >= 0 && rc != 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);
    if (rc != 0 && record) {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (ctx->containers == record)
            ctx->containers = record->next;
        else {
            container_record_t *prev;

            for (prev = ctx->containers; prev && prev->next != record; prev = prev->next) {}
            if (prev)
                prev->next = record->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        pthread_cond_destroy(&record->exited_cond);
        free(record);
    }
    free(child_cfg);
    free(stack);
    return rc;
}

static int build_ps_output(supervisor_ctx_t *ctx, char *buffer, size_t buffer_len)
{
    size_t offset = 0;
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    offset += (size_t)snprintf(buffer + offset,
                               buffer_len - offset,
                               "ID               PID    STATE                REASON               STOP  SOFT(MiB) HARD(MiB) EXIT\n");
    for (record = ctx->containers; record != NULL; record = record->next) {
        unsigned long soft_mib = record->soft_limit_bytes >> 20;
        unsigned long hard_mib = record->hard_limit_bytes >> 20;
        char exit_buf[32];

        if (record->exit_signal != 0)
            snprintf(exit_buf, sizeof(exit_buf), "sig=%d", record->exit_signal);
        else if (record->exit_code >= 0)
            snprintf(exit_buf, sizeof(exit_buf), "code=%d", record->exit_code);
        else
            snprintf(exit_buf, sizeof(exit_buf), "-");

        if (append_text(buffer, buffer_len, &offset,
                        "%-16s %-6d %-20s %-20s %-5s %-9lu %-9lu %s\n",
                        record->id,
                        record->host_pid,
                        state_to_string(record->state),
                        record->final_reason,
                        record->stop_requested ? "yes" : "no",
                        soft_mib,
                        hard_mib,
                        exit_buf) != 0)
            break;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return (int)offset;
}

static int handle_start_or_run(supervisor_ctx_t *ctx,
                               int client_fd,
                               const control_request_t *req,
                               int wait_for_exit)
{
    container_record_t *record = NULL;
    char response_text[CONTROL_MESSAGE_LEN];

    response_text[0] = '\0';

    if (launch_container(ctx, req, &record) != 0) {
        snprintf(response_text, sizeof(response_text), "failed to start %s: %s",
                 req->container_id, strerror(errno));
        return send_response_text(client_fd, 1, response_text);
    }

    snprintf(response_text, sizeof(response_text),
             "started id=%s pid=%d state=%s",
             record->id,
             record->host_pid,
             state_to_string(record->state));

    if (!wait_for_exit)
        return send_response_text(client_fd, 0, response_text);

    pthread_mutex_lock(&ctx->metadata_lock);
    while (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING)
        pthread_cond_wait(&record->exited_cond, &ctx->metadata_lock);

    if (record->state == CONTAINER_STOPPED || record->state == CONTAINER_HARD_LIMIT_KILLED ||
        record->state == CONTAINER_KILLED) {
        if (record->exit_signal != 0)
            snprintf(response_text, sizeof(response_text),
                     "container %s finished: state=%s reason=%s signal=%d",
                     record->id,
                     state_to_string(record->state),
                     record->final_reason,
                     record->exit_signal);
        else
            snprintf(response_text, sizeof(response_text),
                     "container %s finished: state=%s reason=%s",
                     record->id,
                     state_to_string(record->state),
                     record->final_reason);
    } else {
        if (record->exit_signal != 0)
            snprintf(response_text, sizeof(response_text),
                     "container %s exited: state=%s reason=%s signal=%d",
                     record->id,
                     state_to_string(record->state),
                     record->final_reason,
                     record->exit_signal);
        else
            snprintf(response_text, sizeof(response_text),
                     "container %s exited: state=%s reason=%s code=%d",
                     record->id,
                     state_to_string(record->state),
                     record->final_reason,
                     record->exit_code);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return send_response_text(client_fd, record->exit_code >= 0 ? record->exit_code : 0, response_text);
}

static int handle_stop(supervisor_ctx_t *ctx, int client_fd, const control_request_t *req)
{
    container_record_t *record;
    char response_text[CONTROL_MESSAGE_LEN];

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, req->container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(response_text, sizeof(response_text), "container %s not found", req->container_id);
        return send_response_text(client_fd, 1, response_text);
    }

    if (record->state != CONTAINER_STARTING && record->state != CONTAINER_RUNNING) {
        snprintf(response_text, sizeof(response_text),
                 "container %s already finished: state=%s reason=%s",
                 record->id,
                 state_to_string(record->state),
                 record->final_reason);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return send_response_text(client_fd, 0, response_text);
    }

    record->stop_requested = 1;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(record->host_pid, SIGTERM) < 0 && errno != ESRCH)
        fprintf(stderr, "warning: failed to signal container %s: %s\n", record->id, strerror(errno));

    snprintf(response_text, sizeof(response_text), "stop requested for %s", record->id);
    return send_response_text(client_fd, 0, response_text);
}

static int handle_logs(supervisor_ctx_t *ctx, int client_fd, const control_request_t *req)
{
    container_record_t *record;
    char response_text[CONTROL_MESSAGE_LEN];

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, req->container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(response_text, sizeof(response_text), "container %s not found", req->container_id);
        return send_response_text(client_fd, 1, response_text);
    }
    snprintf(response_text, sizeof(response_text), "%s", record->log_path);
    pthread_mutex_unlock(&ctx->metadata_lock);

    return send_response_text(client_fd, 0, response_text);
}

static int handle_request(supervisor_ctx_t *ctx, int client_fd, const control_request_t *req)
{
    char response_text[CONTROL_MESSAGE_LEN];
    int ps_len;

    switch (req->kind) {
    case CMD_START:
        return handle_start_or_run(ctx, client_fd, req, 0);
    case CMD_RUN:
        return handle_start_or_run(ctx, client_fd, req, 1);
    case CMD_PS:
        ps_len = build_ps_output(ctx, response_text, sizeof(response_text));
        if (ps_len < 0)
            ps_len = 0;
        return send_response_text(client_fd, 0, response_text);
    case CMD_LOGS:
        return handle_logs(ctx, client_fd, req);
    case CMD_STOP:
        return handle_stop(ctx, client_fd, req);
    default:
        snprintf(response_text, sizeof(response_text), "unknown command");
        return send_response_text(client_fd, 1, response_text);
    }
}

static void *request_thread_main(void *arg)
{
    request_context_t *request_ctx = arg;
    control_request_t req;

    if (read_full(request_ctx->client_fd, &req, sizeof(req)) == 0)
        (void)handle_request(request_ctx->ctx, request_ctx->client_fd, &req);

    close(request_ctx->client_fd);
    free(request_ctx);
    return NULL;
}

static void signal_handler(int signo)
{
    (void)signo;
    g_shutdown_requested = 1;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;
    int optval = 1;

    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_directory(LOG_DIR) != 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "warning: /dev/container_monitor not available: %s\n", strerror(errno));

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    setsockopt(ctx.server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);
    unlink(CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (listen(ctx.server_fd, 32) < 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.reaper_thread, NULL, reaper_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create reaper");
        g_shutdown_requested = 1;
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    while (!g_shutdown_requested) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);

        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            if (g_shutdown_requested)
                break;
            perror("accept");
            continue;
        }

        request_context_t *request_ctx = calloc(1, sizeof(*request_ctx));
        pthread_t thread;

        if (!request_ctx) {
            close(client_fd);
            continue;
        }

        request_ctx->ctx = &ctx;
        request_ctx->client_fd = client_fd;

        if (pthread_create(&thread, NULL, request_thread_main, request_ctx) == 0)
            pthread_detach(thread);
        else {
            close(client_fd);
            free(request_ctx);
        }
    }

    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *record;

        for (record = ctx.containers; record != NULL; record = record->next) {
            if (record->state == CONTAINER_RUNNING || record->state == CONTAINER_STARTING)
                kill(record->host_pid, SIGKILL);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    pthread_join(ctx.reaper_thread, NULL);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    pthread_mutex_lock(&ctx.metadata_lock);
    while (ctx.containers) {
        container_record_t *record = ctx.containers;

        ctx.containers = record->next;
        pthread_cond_destroy(&record->exited_cond);
        free(record);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int connect_control_socket(void)
{
    struct sockaddr_un addr;
    int fd;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int send_control_request(const control_request_t *req)
{
    int fd = connect_control_socket();
    control_response_t response;

    if (fd < 0) {
        perror("connect");
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != 0) {
        perror("write");
        close(fd);
        return 1;
    }

    if (read_full(fd, &response, sizeof(response)) != 0) {
        perror("read");
        close(fd);
        return 1;
    }

    close(fd);

    if (req->kind == CMD_LOGS) {
        FILE *log_file;
        char buf[512];
        size_t bytes;

        if (response.status != 0) {
            fprintf(stderr, "%s\n", response.message);
            return 1;
        }

        log_file = fopen(response.message, "rb");
        if (!log_file) {
            perror("fopen");
            return 1;
        }

        while ((bytes = fread(buf, 1, sizeof(buf), log_file)) > 0)
            fwrite(buf, 1, bytes, stdout);

        fclose(log_file);
        return 0;
    }

    if (response.message[0] != '\0')
        printf("%s\n", response.message);

    if (req->kind == CMD_RUN)
        return response.status;

    return response.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
