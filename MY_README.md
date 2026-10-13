#MY_README.md 

## Running Test Program
### Single Instance
```
flux start --setattr=log-stderr-level=7

cd ${FLUX_PMIX_DIR}/t

flux run -o verbose=2 -o pmi=pmix src/alloc
```
### Multi-Instance
```
flux start --setattr=log-stderr-level=7

flux alloc --setattr=log-stderr-level=7 -N1

cd ${FLUX_PMIX_DIR}/t

flux run -o verbose=2 -o pmi=pmix src/alloc
```

```
 prterun -np 2 --host n1:1,n2:1 flux start --setattr=log-stderr-level=7 --verbose=2 flux start -c ./bootstrap.toml "flux uri --remote \$FLUX_URI; sleep inf" &
```
with bootstrap.toml:
```
[bootstrap]
curve_cert = "/opt/hpc/build/flux_pmix/t/src/tmp/curve.cert"

default_port = 8050
default_bind = "tcp://*:8050"
default_connect = "tcp://%h:8050"

hosts = [
    { host = "n1" },
    { host = "n2" },
]
```
and cert:

```
mkdir -p /opt/hpc/build/flux_pmix/t/src/tmp
flux keygen /opt/hpc/build/flux_pmix/t/src/tmp/curve.cert
chmod 600 /opt/hpc/build/flux_pmix/t/src/tmp/curve.cert
```

Then export FLUX_URI (last line from flux start output)

# multinode
```
prterun -np 2     --host n1:1,n2:1     flux start     --setattr=log-stderr-level=7     --config-path=bootstrap.toml -v     sleep infinity &

flux uri pid:$(pidof -s flux-broker)

export FLUX_URI=

flux run -n 2 -o verbose=2 -o pmi=pmix ./alloc [extend/release]
```

```
mkdir -p /tmp/flux
flux keygen /tmp/flux/curve.cert
chmod 600 /tmp/flux/curve.cert
```

```

(flux_pmix) [mpiuser@n1 src]$ cat bootstrap.toml 
[bootstrap]
curve_cert = "/opt/hpc/build/flux_pmix/t/src/tmp/curve.cert"

default_port = 8050
default_bind = "tcp://*:8050"
default_connect = "tcp://%h:8050"

hosts = [
    { host = "n1" },
    { host = "n2" },
]
```




## Sequences:

## Start 
-> src/cmd/flux.c

    -> src/cmd/flux-start.c

        -> src/broker/broker.c

## Run

## Alloc
    pmix_client: PMIx_allocation_request
        pmix_server: alloc_server_callback
            pmix_plugin: threadshift, rpc('shell.dyn_alloc_request)
                shell: rpc('job_manager.dyn_alloc_request)
                    job_manager: rpc('job_manager.dyn_alloc_request)
                    ...
                    job_manager rpc('sched.dyn_alloc_request)
                        scheduler DECISION
                    job_manager: response
                    ...
                    job_manager: response
                shell: response
            pmix_plugin: callback
        pmix_server: callback
    pmix_client


## RPC
### Send an rpc request and register cb for response:
static flux_future_t *send_request (flux_t *h, struct cbdata *cbdata)
{
    flux_future_t *f;

    if (!(f = flux_rpc_pack (h,
                             "namespace.dyn_alloc_request",
                             FLUX_NODEID_ANY,
                             0,
                             "{s:b s:b}",
                             "follow", 1,
                             "nobacklog", 1))
        || flux_future_then (f,
                             -1,
                             dyn_alloc_response_cb,
                             cbdata) < 0) {
        flux_future_destroy (f);
        return NULL;
    }
    return f;
}

### Register cb for receiving requests

flux_msg_handler_addvec -> add callbacks for rpc calls

### Unpack request

if (flux_rpc_get_unpack (f, "{s:O}", "data", &xdata) < 0){
    flux_log(ctx->h, LOG_WARNING, "pmix-alloc request: %s", future_strerror (f, errno));
    flux_respond_error(ctx->h, ctx->msg, -1, "unable to unpack response");
    goto out;
}

if(0 > (rc = flux_respond_pack(ctx->h, ctx->msg,  "{s:O}", "data", xdata))){
    flux_log(ctx->h, LOG_WARNING, "flux_respond_pack failed with %d", rc);
    flux_respond_error(ctx->h, ctx->msg, -1, "unable to pack response");
    goto out;
};

