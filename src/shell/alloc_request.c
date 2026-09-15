/************************************************************\
 * Copyright 2026 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* alloc request handling
 *
 * Handle 'shell-<id>.dyn_alloc_request' by forwarding request to job manager
 */
#define FLUX_SHELL_PLUGIN_NAME "alloc_request_handler"

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdio.h>
#include <string.h>
#include <flux/core.h>
#include <flux/shell.h>

#include "builtins.h"

#include <jansson.h>


struct alloc_ctx {
    flux_t *h;
    const flux_msg_t *msg;
};

/* receives RESPONSE from job manager for dynamic allocation request. 
 * forwards RESPONSE to client/pmix_plugin  
 */
static void dyn_alloc_response_cb (flux_future_t *f, void *arg)
{
    json_t *xdata = NULL;
    struct alloc_ctx *ctx = arg;
    int rc;

    shell_debug("received allocation response from job-manager");

    /* TODO: DO whatever is necessary for LEVEL 'shell' before 
     * forwarding response to pmix_plugin 
     */

    if (flux_rpc_get_unpack (f, "{s:o}", "data", &xdata) < 0){
        shell_warn ("pmix-alloc request: %s", future_strerror (f, errno));
        flux_respond_error(ctx->h, ctx->msg, -1, "unable to unpack response");
        goto out;
    }

    if(0 > (rc = flux_respond_pack( ctx->h, 
                                    ctx->msg, 
                                    "{s:i}", 
                                    "type", 1))){
        shell_warn("flux_respond_pack failed with %d", rc);
        flux_respond_error(ctx->h, ctx->msg, -1, "unable to pack response");
        goto out;
    };
    shell_debug("forwarded allocation response");

out:
    free(ctx);
}

/* recives REQUEST from client/pmix_plugin for dynamic allocation request. 
 * forwards REQUEST to job manager  
 */
static void dyn_alloc_request_cb (flux_t *h, flux_msg_handler_t *mh,
                     const flux_msg_t *msg, void *arg)
{
    json_t *spec_add = NULL;
    json_t *spec_sub = NULL;
    flux_future_t *f;
    int target;
    int resource_ref_id;
    char *alloc_ref_id = NULL;
    int directive;

    shell_debug ("received allocation request");
   
    /* TODO: DO whatever is necessary for LEVEL 'shell' before forwarding 
     * request to job manager 
     */



    struct alloc_ctx *ctx = calloc(1, sizeof(*ctx));
    ctx->h = h;
    ctx->msg = flux_msg_incref(msg);


    if(0 < flux_msg_unpack (msg,
                         "{s:i s:s s:i s:i s?:o, s?:o}",
                         "target", &target,
                         "alloc_ref_id", &alloc_ref_id,
                         "resource_ref_id", &resource_ref_id,
                         "directive", &directive,
                         "spec_add", &spec_add,
                         "spec_sub", &spec_sub)){
        
        shell_warn ("error unpacking dynamic allocation request");
        return;
    }

    shell_debug ("forwarding allocation request to job-manager");

    /* Send alloc request to job manager */
    if( !(f = flux_rpc_pack(h, 
                "job-manager.dyn_alloc_request", 
                FLUX_NODEID_ANY, 
                0, 
                "{s:i s:i s:O}",
                "target", target,
                "directive", directive,
                "data", spec_add )) 
        || flux_future_then (f,
                             -1,
                             dyn_alloc_response_cb,
                             (void *) ctx) < 0) 
    {
        if(f) flux_future_destroy (f);
        goto release;

    }
    shell_debug ("forwarded allocation request to job-manager");

release:
    if(alloc_ref_id) free(alloc_ref_id);

}

static int alloc_request_init (flux_plugin_t *p,
                            const char *topic,
                            flux_plugin_arg_t *args,
                            void *data)
{
    flux_shell_t *shell = flux_plugin_get_shell (p);
    if (!shell)
        return -1;
    if (flux_shell_service_register (shell,
                                    "dyn_alloc_request",
                                    dyn_alloc_request_cb,
                                    shell) < 0)
        return -1;
    return 0;
}

struct shell_builtin builtin_alloc_reuest = {
    .name = FLUX_SHELL_PLUGIN_NAME,
    .init = alloc_request_init,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
