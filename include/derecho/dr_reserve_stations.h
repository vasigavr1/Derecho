//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_RESERVE_STATIONS_H
#define ODYSSEY_DR_RESERVE_STATIONS_H



//#include "latency_util.h"
#include "od_generic_inline_util.h"
#include "dr_debug_util.h"

#include <od_inline_util.h>


static inline void fill_prep(context_t *ctx,
                             dr_prepare_t *prep,
                             ctx_trace_op_t *op)
{
  prep->key = op->key;
  memcpy(prep->value, op->value_to_write, (size_t) VALUE_SIZE);
  prep->g_id = assign_new_g_id(ctx);
  if (DEBUG_GID && (prep->g_id % M_2 == 0))
    printf("Wrkr %u assigned g_id %lu \n", ctx->t_id, prep->g_id);
}

static inline void insert_in_w_rob(context_t *ctx,
                                   w_rob_t *w_rob,
                                   dr_prepare_t *prep,
                                   uint16_t session_id,
                                   bool local, uint8_t m_id)
{

  if (ENABLE_ASSERTIONS) {
    if (w_rob->w_state != INVALID) {
      print_w_rob_entry(ctx, w_rob);
      dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
      printf("Active writes %u/%u \n", dr_ctx->gid_rob_arr->size, W_ROB_SIZE);
      assert(false);
    }
  }
  if (local) w_rob->ptr_to_op = prep;
  else
    memcpy(w_rob->value, prep->value, VALUE_SIZE);
  w_rob->is_local = local;
  w_rob->session_id = session_id;
  w_rob->g_id = prep->g_id;
  w_rob->key = prep->key;
  if (ENABLE_ASSERTIONS) assert(w_rob->key.bkt > 0);

  w_rob->w_state = VALID;
}

static inline void insert_in_local_w_rob_ptr(dr_ctx_t *dr_ctx, w_rob_t *w_rob)
{
  w_rob_t **loc_w_rob_ptr = (w_rob_t **) get_fifo_push_slot(dr_ctx->loc_w_rob_ptr);
  (*loc_w_rob_ptr) = w_rob;
  fifo_incr_push_ptr(dr_ctx->loc_w_rob_ptr);
  fifo_increm_capacity(dr_ctx->loc_w_rob_ptr);
}


//Inserts a LOCAL prep to the buffer
static inline void insert_prep_help(context_t *ctx, void* prep_ptr,
                                    void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;

  dr_prepare_t *prep = (dr_prepare_t *) prep_ptr;
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  fill_prep(ctx, prep, op); /// this assigns the g_id

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  dr_prep_mes_t *prep_mes = (dr_prep_mes_t *) get_fifo_push_slot(send_fifo);
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = dr_ctx->inserted_w_id;
    fifo_set_push_backward_ptr(send_fifo, dr_ctx->loc_w_rob_ptr->push_ptr);
  }

  // Bookkeeping
  gid_rob_t *gid_rob = get_and_set_gid_entry(ctx, prep->g_id);
  //print_g_id_rob(ctx, gid_rob->rob_id);
  w_rob_t *w_rob = get_w_rob_ptr(dr_ctx, gid_rob, prep->g_id);
  //print_w_rob_entry(ctx, w_rob);
  insert_in_w_rob(ctx, w_rob, prep, op->session_id, true, ctx->m_id);
  insert_in_local_w_rob_ptr(dr_ctx, w_rob);

  dr_ctx->inserted_w_id++;
  dr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
}

// Inserts REMOTE preps to g_id, w_rob
static inline void fill_dr_ctx_entry(context_t *ctx,
                                     dr_prep_mes_t *prep_mes,
                                     uint8_t prep_i)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  dr_prepare_t *prep = &prep_mes->prepare[prep_i];
  //w_rob_t *w_rob = (w_rob_t *) get_fifo_push_slot(dr_ctx->loc_w_rob_ptr);
  gid_rob_t *gid_rob = get_and_set_gid_entry(ctx, prep->g_id);
  w_rob_t *w_rob = get_w_rob_ptr(dr_ctx, gid_rob, prep->g_id);
  insert_in_w_rob(ctx, w_rob, prep, 0, false, prep_mes->m_id);
}




static inline void reset_dr_ctx_meta(context_t *ctx,
                                     uint16_t *sess_to_free,
                                     uint16_t write_num)
{
  if (write_num == 0) return;
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  dr_ctx->all_sessions_stalled = false;
  for (int w_i = 0; w_i < write_num; ++w_i) {
    uint16_t sess_id = sess_to_free[w_i];
    signal_completion_to_client(sess_id,
                                dr_ctx->index_to_req_array[sess_id],
                                ctx->t_id);
    dr_ctx->stalled[sess_id] = false;
  }
}



static inline void reset_w_rob(w_rob_t *w_rob)
{
  w_rob->w_state = INVALID;
  w_rob->acks_seen = 0;
}

static inline bool dr_write_not_ready(context_t *ctx,
                                      ctx_com_mes_t *com,
                                      w_rob_t * w_rob,
                                      uint64_t g_id,
                                      uint16_t com_i)
{
  bool not_yet_seen = w_rob->w_state != VALID || w_rob->g_id != g_id;
  // it may be that a commit refers to a subset of writes that
  // we have seen and acked, and a subset not yet seen or acked,
  // We need to commit the seen subset to avoid a deadlock

  if (not_yet_seen) {
    com->com_num -= com_i;
    com->l_id += com_i;
    if (ENABLE_STAT_COUNTING)  t_stats[ctx->t_id].received_coms += com_i;
       return true;
  }
  if (DEBUG_COMMITS)
    printf("Wrkr %d valid com %u/%u write with g_id %lu is ready \n",
           ctx->t_id, com_i, com->com_num, g_id);

  return false;
}

#endif //ODYSSEY_DR_RESERVE_STATIONS_H
