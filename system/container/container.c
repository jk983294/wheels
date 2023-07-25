/**
 * sudo apt install libseccomp-dev
 * sudo apt-get install libcap-dev
 * gcc -Wall -Werror -lcap -lseccomp container.c -o container
 *
 * mkdir /tmp/c_mount
 * cp $(which busybox) /tmp/c_mount
 * sudo /home/aaron/cpp/container -m /tmp/c_mount/ -u 0 -c /busybox sh
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <linux/capability.h>
#include <linux/limits.h>
#include <pwd.h>
#include <sched.h>
#include <seccomp.h>  // 安全计算模式
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/capability.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

struct child_config {
    int argc;
    uid_t uid;
    int fd;
    char *hostname;
    char **argv;
    char *mount_dir;
};

int capabilities() {
    fprintf(stderr, "=> dropping capabilities...");
    int drop_caps[] = {
        CAP_AUDIT_CONTROL,  // Allow configuration of audit via unicast netlink socket
        CAP_AUDIT_READ,     // Allow reading the audit log via multicast netlink socket
        CAP_AUDIT_WRITE,    // Allow writing the audit log via unicast netlink socket
        CAP_BLOCK_SUSPEND,  // Allow preventing system suspends
        // DAC (discretionary access control)
        CAP_DAC_READ_SEARCH,  // it is able to read arbitrary file from host system
        CAP_FSETID,           // file set id
        CAP_IPC_LOCK,         // Allow locking of shared memory segments
        CAP_MAC_ADMIN,        // Allow MAC configuration or state changes
        CAP_MAC_OVERRIDE,     // Override MAC access
        // mknod() creates a filesystem node (file, device special file, or named pipe)
        CAP_MKNOD,    // Allow the privileged aspects of mknod
        CAP_SETFCAP,  // set arbitrary capabilities on a file
        CAP_SYSLOG, CAP_SYS_ADMIN,
        CAP_SYS_BOOT,      // Allow use of reboot
        CAP_SYS_MODULE,    // Load and unload kernel modules
        CAP_SYS_NICE,      // Allow raising priority and setting priority on other process
        CAP_SYS_RAWIO,     // Perform I/O port operations
        CAP_SYS_RESOURCE,  // Set resource limits
        CAP_SYS_TIME,      // manipulation of system clock
        CAP_WAKE_ALARM     // Allow triggering something that will wake the system
    };
    size_t num_caps = sizeof(drop_caps) / sizeof(*drop_caps);
    fprintf(stderr, "bounding...");
    for (size_t i = 0; i < num_caps; i++) {
        if (prctl(PR_CAPBSET_DROP, drop_caps[i], 0, 0, 0)) {
            fprintf(stderr, "prctl failed: %m\n");
            return 1;
        }
    }
    fprintf(stderr, "inheritable...");
    cap_t caps = NULL;
    // clearing the inheritable and ambient sets
    if (!(caps = cap_get_proc()) || cap_set_flag(caps, CAP_INHERITABLE, num_caps, drop_caps, CAP_CLEAR) ||
        cap_set_proc(caps)) {
        fprintf(stderr, "failed: %m\n");
        if (caps) cap_free(caps);
        return 1;
    }
    cap_free(caps);
    fprintf(stderr, "done.\n");
    return 0;
}

int pivot_root(const char *new_root, const char *put_old) { return syscall(SYS_pivot_root, new_root, put_old); }

int mounts(struct child_config *config) {
    /**
     * Create a temporary directory, and one inside of it.
     * Bind mount of the user argument onto the temporary directory
     * A bind mount makes a directory subtree visible at another point within the single directory hierarchy.
     * pivot_root, making the bind mount our root and mounting the old root onto the inner temporary directory.
     * umount the old root, and remove the inner temporary directory.
     */
    fprintf(stderr, "=> remounting everything with MS_PRIVATE...");
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL)) {
        fprintf(stderr, "failed! %m\n");
        return -1;
    }
    fprintf(stderr, "remounted.\n");

    fprintf(stderr, "=> making a temp directory and a bind mount there...\n");
    char mount_dir[] = "/tmp/tmp.XXXXXX";
    if (!mkdtemp(mount_dir)) {
        fprintf(stderr, "failed making a directory!\n");
        return -1;
    }

    if (mount(config->mount_dir, mount_dir, NULL, MS_BIND | MS_PRIVATE, NULL)) {
        fprintf(stderr, "bind mount file=%s dir=%s failed! msg=%s\n", config->mount_dir, mount_dir, strerror(errno));
        return -1;
    } else {
        fprintf(stderr, "bind mount %s success!\n", mount_dir);
    }

    char inner_mount_dir[] = "/tmp/tmp.XXXXXX/oldroot.XXXXXX";
    memcpy(inner_mount_dir, mount_dir, sizeof(mount_dir) - 1);
    if (!mkdtemp(inner_mount_dir)) {
        fprintf(stderr, "failed making the inner directory!\n");
        return -1;
    } else {
        fprintf(stderr, "mkdtemp inner_mount_dir %s success!\n", inner_mount_dir);
    }

    fprintf(stderr, "=> pivoting root...");
    if (pivot_root(mount_dir, inner_mount_dir)) {  // changes the root directory and the current working directory
        fprintf(stderr, "failed!\n");
        return -1;
    } else {
        fprintf(stderr, "pivot_root success!\n");
    }

    char *old_root_dir = basename(inner_mount_dir);
    char old_root[sizeof(inner_mount_dir) + 1] = {"/"};
    strcpy(&old_root[1], old_root_dir);

    fprintf(stderr, "=> unmounting old_root %s...\n", old_root);
    if (chdir("/")) {
        fprintf(stderr, "chdir failed! %m\n");
        return -1;
    }
    if (umount2(old_root, MNT_DETACH)) {
        fprintf(stderr, "umount failed! %m\n");
        return -1;
    } else {
        fprintf(stderr, "umount2(old_root, MNT_DETACH) %s success!\n", old_root);
    }
    if (rmdir(old_root)) {
        fprintf(stderr, "rmdir failed! %m\n");
        return -1;
    } else {
        fprintf(stderr, "rmdir(old_root) %s success!\n", old_root);
    }
    return 0;
}

#define SCMP_FAIL SCMP_ACT_ERRNO(EPERM)

int syscalls() {
    scmp_filter_ctx ctx = NULL;
    fprintf(stderr, "=> filtering syscalls...");
    if (!(ctx = seccomp_init(SCMP_ACT_ALLOW)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(chmod), 1, SCMP_A1(SCMP_CMP_MASKED_EQ, S_ISUID, S_ISUID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(chmod), 1, SCMP_A1(SCMP_CMP_MASKED_EQ, S_ISGID, S_ISGID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(fchmod), 1, SCMP_A1(SCMP_CMP_MASKED_EQ, S_ISUID, S_ISUID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(fchmod), 1, SCMP_A1(SCMP_CMP_MASKED_EQ, S_ISGID, S_ISGID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(fchmodat), 1, SCMP_A2(SCMP_CMP_MASKED_EQ, S_ISUID, S_ISUID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(fchmodat), 1, SCMP_A2(SCMP_CMP_MASKED_EQ, S_ISGID, S_ISGID)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(unshare), 1,
                         SCMP_A0(SCMP_CMP_MASKED_EQ, CLONE_NEWUSER, CLONE_NEWUSER)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(clone), 1,
                         SCMP_A0(SCMP_CMP_MASKED_EQ, CLONE_NEWUSER, CLONE_NEWUSER)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(ioctl), 1, SCMP_A1(SCMP_CMP_MASKED_EQ, TIOCSTI, TIOCSTI)) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(keyctl), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(add_key), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(request_key), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(ptrace), 0) || seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(mbind), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(migrate_pages), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(move_pages), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(set_mempolicy), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(userfaultfd), 0) ||
        seccomp_rule_add(ctx, SCMP_FAIL, SCMP_SYS(perf_event_open), 0) ||
        seccomp_attr_set(ctx, SCMP_FLTATR_CTL_NNP, 0) || seccomp_load(ctx)) {
        if (ctx) seccomp_release(ctx);
        fprintf(stderr, "failed: %m\n");
        return 1;
    }
    seccomp_release(ctx);
    fprintf(stderr, "done.\n");
    return 0;
}

#define MEMORY "1073741824"
#define SHARES "256"
#define PIDS "64"
#define WEIGHT "10"
#define FD_COUNT 64

struct cgrp_control {
    char control[256];
    struct cgrp_setting {
        char name[256];
        char value[256];
    } * *settings;
};
struct cgrp_setting add_to_tasks = {.name = "tasks", .value = "0"};

struct cgrp_control *cgrps[] = {
    &(struct cgrp_control){
        .control = "memory",
        .settings =
            (struct cgrp_setting *[]){
                // the contained process and its child processes can't total more than 1GB memory in userspace
                &(struct cgrp_setting){.name = "memory.limit_in_bytes", .value = MEMORY},
                // the contained process and its child processes can't total more than 1GB memory in userspace
                &(struct cgrp_setting){.name = "memory.kmem.limit_in_bytes", .value = MEMORY}, &add_to_tasks, NULL}},
    &(struct cgrp_control){.control = "cpu",
                           .settings =
                               (struct cgrp_setting *[]){
                                   // the contained process take a quarter of cpu-time on a busy system at most
                                   &(struct cgrp_setting){.name = "cpu.shares", .value = SHARES}, &add_to_tasks, NULL}},
    &(struct cgrp_control){
        .control = "pids",
        .settings =
            (struct cgrp_setting *[]){// allowing the contained process and its children to have 64 pids at most.
                                      &(struct cgrp_setting){.name = "pids.max", .value = PIDS}, &add_to_tasks, NULL}},
    //    &(struct cgrp_control){
    //        .control = "blkio",
    //        .settings = (struct cgrp_setting *[]){&(struct cgrp_setting){.name = "blkio.weight", .value = PIDS},
    //                                              &add_to_tasks, NULL}
    //    },
    NULL};

int resources(struct child_config *config) {
    fprintf(stderr, "=> setting cgroups...");
    for (struct cgrp_control **cgrp = cgrps; *cgrp; cgrp++) {
        char dir[PATH_MAX] = {0};
        fprintf(stderr, "%s...", (*cgrp)->control);
        if (snprintf(dir, sizeof(dir), "/sys/fs/cgroup/%s/%s", (*cgrp)->control, config->hostname) == -1) {
            return -1;
        }
        if (mkdir(dir, S_IRUSR | S_IWUSR | S_IXUSR)) {
            fprintf(stderr, "mkdir %s failed: %m\n", dir);
            return -1;
        } else {
            fprintf(stderr, "mkdir %s good\n", dir);
        }
        for (struct cgrp_setting **setting = (*cgrp)->settings; *setting; setting++) {
            char path[PATH_MAX] = {0};
            int fd = 0;
            if (snprintf(path, sizeof(path), "%s/%s", dir, (*setting)->name) == -1) {
                fprintf(stderr, "snprintf failed: %m\n");
                return -1;
            }
            if ((fd = open(path, O_WRONLY)) == -1) {
                fprintf(stderr, "opening %s failed: %m\n", path);
                return -1;
            } else {
                fprintf(stderr, "opening %s good\n", dir);
            }
            if (write(fd, (*setting)->value, strlen((*setting)->value)) == -1) {
                fprintf(stderr, "writing to %s failed: %m\n", path);
                close(fd);
                return -1;
            } else {
                fprintf(stderr, "writing to %s good\n", dir);
            }
            close(fd);
        }
    }
    fprintf(stderr, "done.\n");
    fprintf(stderr, "=> setting rlimit...");

    // lower the hard limit on the number of file descriptors
    if (setrlimit(RLIMIT_NOFILE, &(struct rlimit){
                                     .rlim_max = FD_COUNT,
                                     .rlim_cur = FD_COUNT,
                                 })) {
        fprintf(stderr, "failed: %m\n");
        return 1;
    }
    fprintf(stderr, "done.\n");
    return 0;
}

int free_resources(struct child_config *config) {
    fprintf(stderr, "=> cleaning cgroups...");
    for (struct cgrp_control **cgrp = cgrps; *cgrp; cgrp++) {
        char dir[PATH_MAX] = {0};
        char task[PATH_MAX] = {0};
        int task_fd = 0;
        if (snprintf(dir, sizeof(dir), "/sys/fs/cgroup/%s/%s", (*cgrp)->control, config->hostname) == -1 ||
            snprintf(task, sizeof(task), "/sys/fs/cgroup/%s/tasks", (*cgrp)->control) == -1) {
            fprintf(stderr, "snprintf failed: %m\n");
            return -1;
        }
        if ((task_fd = open(task, O_WRONLY)) == -1) {
            fprintf(stderr, "opening %s failed: %m\n", task);
            return -1;
        }
        if (write(task_fd, "0", 2) == -1) {
            fprintf(stderr, "writing to %s failed: %m\n", task);
            close(task_fd);
            return -1;
        }
        close(task_fd);
        if (rmdir(dir)) {
            fprintf(stderr, "rmdir %s failed: %m", dir);
            return -1;
        }
    }
    fprintf(stderr, "done.\n");
    return 0;
}

#define USERNS_OFFSET 10000
#define USERNS_COUNT 2000

int handle_child_uid_map(pid_t child_pid, int fd) {  // child
    int uid_map = 0;
    int has_userns = -1;
    if (read(fd, &has_userns, sizeof(has_userns)) != sizeof(has_userns)) {
        fprintf(stderr, "couldn't read from child!\n");
        return -1;
    }
    if (has_userns) {
        char path[PATH_MAX] = {0};
        for (char **file = (char *[]){"uid_map", "gid_map", 0}; *file; file++) {
            if (snprintf(path, sizeof(path), "/proc/%d/%s", child_pid, *file) > sizeof(path)) {
                fprintf(stderr, "snprintf too big? %m\n");
                return -1;
            }
            fprintf(stderr, "writing %s...", path);
            if ((uid_map = open(path, O_WRONLY)) == -1) {
                fprintf(stderr, "open failed: %m\n");
                return -1;
            }
            if (dprintf(uid_map, "0 %d %d\n", USERNS_OFFSET, USERNS_COUNT) == -1) {
                fprintf(stderr, "dprintf failed: %m\n");
                close(uid_map);
                return -1;
            }
            close(uid_map);
        }
    }
    if (write(fd, &(int){0}, sizeof(int)) != sizeof(int)) {
        fprintf(stderr, "couldn't write: %m\n");
        return -1;
    }
    return 0;
}
int userns(struct child_config *config) {  // child
    fprintf(stderr, "=> trying a user namespace...");
    int has_userns = !unshare(CLONE_NEWUSER);
    if (write(config->fd, &has_userns, sizeof(has_userns)) != sizeof(has_userns)) {
        fprintf(stderr, "couldn't write: %m\n");
        return -1;
    }
    int result = 0;
    if (read(config->fd, &result, sizeof(result)) != sizeof(result)) {
        fprintf(stderr, "couldn't read: %m\n");
        return -1;
    }
    if (result) return -1;
    if (has_userns) {
        fprintf(stderr, "done.\n");
    } else {
        fprintf(stderr, "unsupported? continuing.\n");
    }
    fprintf(stderr, "=> switching to uid %d / gid %d...", config->uid, config->uid);
    if (setgroups(1, &(gid_t){config->uid}) || setresgid(config->uid, config->uid, config->uid) ||
        setresuid(config->uid, config->uid, config->uid)) {
        fprintf(stderr, "%m\n");
        return -1;
    }
    fprintf(stderr, "done.\n");
    return 0;
}
int child_entry(void *arg) {
    struct child_config *config = arg;
    if (sethostname(config->hostname, strlen(config->hostname)) || mounts(config) || userns(config) || capabilities() ||
        syscalls()) {
        close(config->fd);
        return -1;
    }
    if (close(config->fd)) {
        fprintf(stderr, "close failed: %m\n");
        return -1;
    }
    if (execve(config->argv[0], config->argv, NULL)) {  // run child executable
        fprintf(stderr, "execve failed! %s error=%m\n", config->argv[0]);
        return -1;
    }
    return 0;
}

void print_usage() {
    fprintf(stderr, "Usage: ./container -u -1 -m . -c /bin/sh ~\n");
    fprintf(stderr, "\t\t-u user id\n");
    fprintf(stderr, "\t\t-m mount dir\n");
    fprintf(stderr, "\t\t-c child process config\n");
    exit(1);
}

void cleanup_and_exit(int *sockets) {
    if (sockets[0]) close(sockets[0]);
    if (sockets[1]) close(sockets[1]);
    exit(1);
}

void free_resources_(char *stack, struct child_config *config, int err) {
    free_resources(config);
    free(stack);
    exit(err);
}

int main(int argc, char **argv) {
    struct child_config config = {0};
    int err = 0;
    int option = 0;
    int sockets[2] = {0};
    pid_t child_pid = 0;
    int last_optind = 0;
    while ((option = getopt(argc, argv, "c:m:u:"))) {
        switch (option) {
            case 'c':
                config.argc = argc - last_optind - 1;
                config.argv = &argv[argc - config.argc];
                goto finish_options;
            case 'm':
                config.mount_dir = optarg;
                break;
            case 'u':
                if (sscanf(optarg, "%d", &config.uid) != 1) {
                    fprintf(stderr, "badly-formatted uid: %s\n", optarg);
                    print_usage();
                }
                break;
            default:
                print_usage();
        }
        last_optind = optind;
    }
finish_options:
    if (!config.argc) print_usage();
    if (!config.mount_dir) print_usage();

    fprintf(stderr, "=> validating Linux version...");
    struct utsname host;
    memset(&host, 0, sizeof(host));
    if (uname(&host)) {
        fprintf(stderr, "failed: %m\n");
        cleanup_and_exit(sockets);
    }
    int major = -1;
    int minor = -1;
    if (sscanf(host.release, "%u.%u.", &major, &minor) != 2) {
        fprintf(stderr, "weird release format: %s\n", host.release);
        cleanup_and_exit(sockets);
    }
    //    if (major != 4 || (minor != 7 && minor != 8)) {
    //        fprintf(stderr, "expected 4.7.x or 4.8.x: %s\n", host.release);
    //        cleanup_and_exit(sockets);
    //    }
    if (strcmp("x86_64", host.machine)) {
        fprintf(stderr, "expected x86_64: %s\n", host.machine);
        cleanup_and_exit(sockets);
    }
    fprintf(stderr, "%s on %s.\n", host.release, host.machine);

    char hostname[256] = "my_virtual_host";
    config.hostname = hostname;

    if (socketpair(AF_LOCAL, SOCK_SEQPACKET, 0, sockets)) {  // used for child sending some msgs to parent
        fprintf(stderr, "socketpair failed: %m\n");
        cleanup_and_exit(sockets);
    }
    if (fcntl(sockets[0], F_SETFD, FD_CLOEXEC)) {  // the child only receives access to one
        fprintf(stderr, "fcntl failed: %m\n");
        cleanup_and_exit(sockets);
    }
    config.fd = sockets[1];
#define STACK_SIZE (1024 * 1024)

    char *stack = (char *)malloc(STACK_SIZE);
    if (!stack) {
        fprintf(stderr, "=> malloc failed, out of memory?\n");
        cleanup_and_exit(sockets);
    }
    if (resources(&config)) {
        free_resources_(stack, &config, 1);
    }
    int flags = CLONE_NEWNS        // create new namespace
                | CLONE_NEWCGROUP  // New cgroup namespace
                | CLONE_NEWPID     // New pid namespace
                | CLONE_NEWIPC     // New ipcs
                | CLONE_NEWNET     // New network namespace
                | CLONE_NEWUTS;    // New utsname group

    /**
     * child process will run child_entry and return child_pid
     * stack + STACK_SIZE means we point to the topmost address of the stack memory
     */
    if ((child_pid = clone(child_entry, stack + STACK_SIZE, flags | SIGCHLD, &config)) == -1) {
        fprintf(stderr, "=> clone failed! %m\n");
        free_resources_(stack, &config, 1);
    }

    fprintf(stderr, "child_pid=%d\n", child_pid);

    /**
     * Close and zero the child's socket, so that if something breaks then we don't leave an open fd,
     * possibly causing the child to or the parent to hang.
     */
    close(sockets[1]);
    sockets[1] = 0;
    if (handle_child_uid_map(child_pid, sockets[0])) {
        err = 1;
        if (child_pid) kill(child_pid, SIGKILL);
    }

    int child_status = 0;
    waitpid(child_pid, &child_status, 0);  // parent wait for child finish
    err |= WEXITSTATUS(child_status);
    free_resources_(stack, &config, err);
    return err;
}
