/*
 * flux-partitioning-shell-plugin.c
 *
 * Node-local Flux shell plugin for resource partitioning.
 */

#define _GNU_SOURCE
#define FLUX_SHELL_PLUGIN_NAME "partitioning"

#include <flux/shell.h>
#include <jansson.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>


/* ========================================================= */
/* Configuration                                             */
/* ========================================================= */

#define REGISTRY_DIR  "/tmp/flux-partitioning"

#define MAX_CORE_PARTITIONS 256


/* Information about ONE application on THIS node. */
typedef struct {
    uint64_t jobid;

    char appname[128];
    int broker_rank;
    int local_core_count;
    char local_core_list[256];

} app_info_t;


/* Internal representation of one partitioning decision.*/
typedef struct {
    uint8_t coreGroup;
    uint8_t cache;
    uint8_t memory;

} core_partition_t;


/* Complete response from the Decision Block. */
typedef struct {
    core_partition_t partitions[MAX_CORE_PARTITIONS];
    int count;
} partition_response_t;


/* ========================================================= */
/* Application name                                          */
/* ========================================================= */

static void parse_app_name(json_t *jobspec, char *buf, size_t buflen) {
    snprintf(buf, buflen, "unknown");

    if (!jobspec) {
        return;
    }

    json_t *tasks = json_object_get(jobspec, "tasks");
    if (!json_is_array(tasks)) {
        return;
    }
    if (json_array_size(tasks) == 0){
        return;
    }

    json_t *task = json_array_get(tasks, 0);
    if (!task) {
        return;
    }

    json_t *command = json_object_get(task, "command");
    if (!json_is_array(command)) {
        return;
    }
    if (json_array_size(command) == 0) {
        return;
    }

    json_t *argv0 = json_array_get(command, 0);
    if (!json_is_string(argv0)) {
        return;
    }

    const char *command_name = json_string_value(argv0);
    if (!command_name) {
        return;
    }

    const char *base = strrchr(command_name, '/');
    if (base) {
        base++;
    } else {
        base = command_name;
    }

    snprintf(buf, buflen, "%s", base);
}


/* ========================================================= */
/* Get application information for THIS node                 */
/* ========================================================= */

static int getPartitionDetails(flux_plugin_t *p, app_info_t *info) {
    flux_shell_t *shell = flux_plugin_get_shell(p);

    if (!shell) {
        return -1;
    }

    json_t *jobspec = NULL;
    int shell_rank = -1;
    int broker_rank = -1;
    int local_core_count = 0;
    const char *local_cores = NULL;


    memset(info, 0, sizeof(*info));
    snprintf(info->appname, sizeof(info->appname), "unknown");
    snprintf(info->local_core_list, sizeof(info->local_core_list), "unknown");

    if (flux_shell_info_unpack(
        shell,
        "{s:I s:i s:o}",
        "jobid", &info->jobid,
        "rank", &shell_rank,
        "jobspec", &jobspec) < 0) {

        shell_log_errno("flux_shell_info_unpack");
        return -1;
    }

    parse_app_name(jobspec, info->appname, sizeof(info->appname));

    if (flux_shell_rank_info_unpack(
            shell,
            shell_rank,
            "{s:i s:{s:i s:s}}",
            "broker_rank", &broker_rank,
            "resources",
                "ncores", &local_core_count,
                "cores", &local_cores) < 0) {
        shell_log_errno("flux_shell_rank_info_unpack");
        return -1;
    }

    info->broker_rank = broker_rank;
    if (!local_cores) {
        shell_log("Unable to determine local cores for jobid=%" PRIu64, info->jobid);
        return -1;
    }

    snprintf(info->local_core_list, sizeof(info->local_core_list), "%s", local_cores);

    info->local_core_count = local_core_count;
    shell_log("Local application: "
            "jobid=%" PRIu64 " app=%s"
            " shell_rank=%d broker_rank=%d"
            " cores=%s"
            " local_core_count=%d",
            info->jobid,
            info->appname,
            shell_rank,
            broker_rank,
            info->local_core_list,
            info->local_core_count);

    return 0;
}


/* ========================================================= */
/* Node-local active application registry                    */
/* ========================================================= */
static int mkdir_registry_dir(void) {
    if (mkdir(REGISTRY_DIR, 0777) < 0) {
        if (errno != EEXIST) {
            return -1;
        }
    }

    return 0;
}


static json_t *load_registry(const char *registry_file) {
    json_error_t error;

    json_t *root = json_load_file(registry_file, 0, &error);

    if (!root) {
        root = json_object();
        if (!root) {
            return NULL;
        }
        json_object_set_new(root, "active_apps", json_array());
    }


    json_t *apps = json_object_get(root, "active_apps");
    if (!json_is_array(apps)) {
        json_object_set_new(root, "active_apps", json_array());
    }

    return root;
}


static int save_registry(const char *registry_file, int broker_rank, json_t *root) {
    char tmpfile[PATH_MAX];

    snprintf(tmpfile, sizeof(tmpfile), REGISTRY_DIR "/active-apps-%d.json.tmp.%ld", broker_rank, (long)getpid());
    if (json_dump_file( root, tmpfile, JSON_INDENT(2)) < 0) {
        return -1;
    }

    if (rename(tmpfile, registry_file) < 0) {
        unlink(tmpfile);
        return -1;
    }
    return 0;
}


static void remove_job_from_registry(json_t *apps, uint64_t jobid) {
    size_t index = 0;

    while (index < json_array_size(apps)) {
        json_t *entry = json_array_get(apps, index);
        json_t *jobid_json = json_object_get(entry, "jobid");

        if (json_is_integer(jobid_json) &&(uint64_t) json_integer_value(jobid_json) == jobid) {
            json_array_remove(apps, index);
            continue;
        }
        index++;
    }
}


static int update_registry(const char *event, const app_info_t *info, int broker_rank, json_t **active_apps_out) {
    if (!active_apps_out) {
        return -1;
    }
    *active_apps_out = NULL;

    char registry_file[PATH_MAX];
    char registry_lock[PATH_MAX];
    snprintf(registry_file, sizeof(registry_file), "%s/active-apps-%d.json", REGISTRY_DIR, broker_rank);
    snprintf(registry_lock, sizeof(registry_lock), "%s/active-apps-%d.lock", REGISTRY_DIR, broker_rank);

    if (mkdir_registry_dir() < 0) {
        return -1;
    }

    int lockfd = open(registry_lock, O_CREAT | O_RDWR, 0666);
    if (lockfd < 0) {
        return -1;
    }

    if (flock(lockfd, LOCK_EX) < 0) {
        close(lockfd);
        return -1;
    }

    json_t *root = load_registry(registry_file);
    if (!root) {
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }

    json_t *apps = json_object_get(root, "active_apps");

    remove_job_from_registry(apps, info->jobid);

    if (strcmp(event, "start") == 0) {
        json_t *entry =
            json_pack(
                "{s:I s:s s:i s:s}",

                "jobid",
                (json_int_t)
                info->jobid,

                "appname",
                info->appname,

                "local_core_count",
                info->local_core_count,

                "local_core_list",
                info->local_core_list);


        if (!entry) {
            json_decref(root);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            return -1;
        }


        json_array_append_new(apps, entry);
    }

    if (save_registry(registry_file, broker_rank, root) < 0) {
        json_decref(root);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }

    *active_apps_out = json_deep_copy(apps);
    json_decref(root);
    flock(lockfd, LOCK_UN);
    close(lockfd);

    return *active_apps_out ? 0 : -1;
}


/* ========================================================= */
/* Debug helper                                              */
/* ========================================================= */
static void log_active_apps(json_t *active_apps) {
    char *text = json_dumps( active_apps, JSON_COMPACT);
    shell_log("Node active applications: %s", text ? text : "null");
    free(text);
}


/* ========================================================= */
/* ABSTRACT MESSAGE #1                                       */
/* Plugin -> Partitioning Decision Block                     */
/* Decision Block -> Plugin                                  */
/* ========================================================= */
static void add_core_partitions(const char *core_list, partition_response_t *response) {
    char tmp[256];

    if (!core_list || !response) {
        return;
    }

    snprintf(tmp, sizeof(tmp), "%s", core_list);

    char *saveptr = NULL;
    char *token = strtok_r(tmp, ",", &saveptr);

    while (token && response->count < MAX_CORE_PARTITIONS) {
        int first;
        int last;

        if (sscanf(token, "%d-%d", &first, &last) == 2) {
            for (int core = first;
                 core <= last && response->count < MAX_CORE_PARTITIONS;
                 core++) {
                if (core < 0 || core > UINT8_MAX) {
                    continue;
                }

                core_partition_t *p = &response->partitions[response->count++];
                p->coreGroup = (uint8_t)core;
                p->cache = 50;
                p->memory = 100;
            }
        } else if (sscanf(token, "%d", &first) == 1) {
            if (first >= 0 && first <= UINT8_MAX) {
                core_partition_t *p = &response->partitions[response->count++];
                p->coreGroup = (uint8_t)first;
                p->cache = 50;
                p->memory = 100;
            }
        }

        token = strtok_r(NULL, ",", &saveptr);
    }
}

static int requestPartitioningScheme(const char *event, const app_info_t *trigger_app, json_t *active_apps, partition_response_t *response)
{
    if (!event || !trigger_app || !active_apps || !response) {
        return -1;
    }

    memset(response, 0, sizeof(*response));
    shell_log("requestPartitioningScheme: event=%s trigger_jobid=%" PRIu64, event, trigger_app->jobid);
    log_active_apps(active_apps);
    size_t index;
    json_t *app;

    json_array_foreach(active_apps, index, app) {
        const char *core_list = NULL;

        if (json_unpack(app,
                        "{s:s}",
                        "local_core_list", &core_list) < 0) {
            shell_log("Invalid application entry in active_apps");
            return -1;
        }

        add_core_partitions(core_list, response);
    }

    shell_log("Mock Decision Block returned %d partition entries", response->count);

    return 0;
}


static int sendPartitioningMessage(const core_partition_t *partition) {
    if (!partition) {
        return -1;
    }

    shell_log(
        "SEND SetReqPartitioningV1: "
        "coreGroup=%u "
        "cache=%u "
        "memory=%u",

        (unsigned)
        partition->coreGroup,

        (unsigned)
        partition->cache,

        (unsigned)
        partition->memory);

    return 0;
}


static int forwardPartitioningScheme(const partition_response_t *response) {
    if (!response) {
        return -1;
    }

    shell_log("Forwarding %d partition messages " "to Access Daemon", response->count);

    for (int i = 0; i < response->count; i++) {
        const core_partition_t *partition = &response->partitions[i];

        if (sendPartitioningMessage(partition) < 0) {
            shell_log(
                "Failed to send "
                "SetReqPartitioningV1 "
                "for coreGroup=%u",

                (unsigned)
                partition->coreGroup);

            return -1;
        }
    }
    return 0;
}


static int partitioning_prolog_cb(flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *data) {
    app_info_t info;
    json_t *active_apps = NULL;
    partition_response_t partitioning;

    if (getPartitionDetails(p, &info) < 0) {
        shell_log("getPartitionDetails failed");
        return -1;
    }

    if (update_registry("start", &info, info.broker_rank, &active_apps) < 0) {
        shell_log("update_registry(start) failed");
        return -1;
    }

    if (requestPartitioningScheme("start", &info, active_apps, &partitioning) < 0) {
        shell_log("requestPartitioningScheme(start) failed");
        json_decref(active_apps);
        return -1;
    }

    if (forwardPartitioningScheme(&partitioning) < 0) {
        shell_log("forwardPartitioningScheme(start) failed");
        json_decref(active_apps);
        return -1;
    }

    json_decref(active_apps);

    return 0;
}

static int partitioning_epilog_cb(flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *data) {
    app_info_t info;
    json_t *active_apps = NULL;
    partition_response_t partitioning;

    if (getPartitionDetails(p, &info) < 0) {
        shell_log("getPartitionDetails failed");
        return -1;
    }

    if (update_registry("finish", &info, info.broker_rank, &active_apps) < 0) {
        shell_log("update_registry(finish) failed");
        return -1;
    }

    if (requestPartitioningScheme("finish", &info, active_apps, &partitioning) < 0) {
        shell_log("requestPartitioningScheme(finish) failed");
        json_decref(active_apps);
        return -1;
    }

    if (forwardPartitioningScheme( &partitioning) < 0) {
        shell_log("forwardPartitioningScheme(finish) failed");
        json_decref(active_apps);
        return -1;
    }

    json_decref(active_apps);

    return 0;
}


/* ========================================================= */
/* Plugin initialization                                     */
/* ========================================================= */

int flux_plugin_init(flux_plugin_t *p)
{
    if (flux_plugin_add_handler(p, "shell.init", partitioning_prolog_cb, NULL) < 0) {
        return -1;
    }

    if (flux_plugin_add_handler(p, "shell.finish", partitioning_epilog_cb, NULL) < 0) {
        return -1;
    }

    return 0;
}