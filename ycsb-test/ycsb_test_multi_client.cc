#include <bits/stdint-uintn.h>
#include <stdio.h>
#include <stdlib.h>

#include <atomic>

#include "client.h"
#include "ycsb_test.h"
#include "hiredis/hiredis.h"
#include "client_mm.h"

int main(int argc, char ** argv) {
    if (argc != 5) {
        printf("Usage: %s path-to-config-file workload-name num-clients methods\n", argv[0]);
        return 1;
    }
    alloc_method alloc_method_ ;

    WorkloadFileName * workload_fnames = get_workload_fname(argv[2]);
    int num_clients = atoi(argv[3]);
    std::string allocator_type = argv[4];
    if (allocator_type == "cxl")
        alloc_method_ = cxl_shm_alloc;
    else if (allocator_type == "fusee") 
        alloc_method_ = fusee_alloc;
    else if (allocator_type == "share")
        alloc_method_  = share_alloc;
    else if (allocator_type == "pool")
        alloc_method_  = pool_alloc;
   
    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);

    // bind this process to main core
    // run client args
    RunClientArgs * client_args_list = (RunClientArgs *)malloc(sizeof(RunClientArgs) * num_clients);
    pthread_barrier_t global_load_barrier;
    pthread_barrier_init(&global_load_barrier, NULL, num_clients);
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;

    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id     = config.server_id - config.memory_num;
        client_args_list[i].thread_id     = i;
        client_args_list[i].main_core_id  = config.main_core_id + i * 2;
        client_args_list[i].poll_core_id  = config.poll_core_id + i * 2;
        client_args_list[i].workload_name = argv[2];
        client_args_list[i].config_file   = argv[1];
        client_args_list[i].load_barrier  = &global_load_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].ret_num_ops = 0;
        client_args_list[i].ret_faile_num = 0;
        client_args_list[i].num_threads = num_clients;
        client_args_list[i].client_alloc_method_ = alloc_method_;

        pthread_t tid;
        pthread_create(&tid, NULL, run_client, &client_args_list[i]);
        tid_list[i] = tid;
    }

    uint64_t total_tpt = 0;
    uint32_t total_failed = 0;
    uint64_t total_freed = 0;
    double total_ratio = 0;
    uint64_t total_lat[1000] = {0};
    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        total_tpt += client_args_list[i].ret_num_ops;
        total_failed += client_args_list[i].ret_faile_num;
        total_freed += client_args_list[i].free_size;
        total_ratio += client_args_list[i].ratio;
        for(int j = 0; j < 1000; j ++) {
            total_lat[j] += client_args_list[i].ret_lat[j];
        }
    }
    printf("total: %d ops\n", total_tpt);
    printf("failed: %d ops\n", total_failed);
    printf("freed: %lu MiB\n", total_freed/1024/1024);
    printf("free ratio: %lf\n", total_ratio/num_clients);
    printf("tpt: %d ops/s\n", (total_tpt - total_failed) / config.workload_run_time);
    FILE * lat_fp = fopen("result", "w");
    assert(lat_fp != NULL);
    for (int i = 0; i < 1000; i ++) {
        fprintf(lat_fp, "%ld\n", total_lat[i]);
    }
    fprintf(lat_fp, "%ld\n",(total_tpt-total_failed)/config.workload_run_time);
    fclose(lat_fp);
    redisContext *redis_conn;
    redisReply *redis_reply;
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    redis_conn = redisConnectWithTimeout("10.10.1.1", 2222, timeout);
    redis_reply = (redisReply*)redisCommand(redis_conn, "INCRBYFLOAT avg %s", std::to_string((double)(total_tpt - total_failed) / config.workload_run_time).c_str());
    printf("INCUR: %s\n", redis_reply->str);
    freeReplyObject(redis_reply);
    redis_reply = (redisReply*)redisCommand(redis_conn, "INCR finished");
    freeReplyObject(redis_reply);

}
