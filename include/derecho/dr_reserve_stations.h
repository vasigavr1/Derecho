//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_RESERVE_STATIONS_H
#define ODYSSEY_DR_RESERVE_STATIONS_H



//#include "latency_util.h"
#include "generic_inline_util.h"
#include "dr_debug_util.h"

#include <inline_util.h>


static inline void fill_prep(context_t *ctx,
                             dr_prepare_t *prep,
                             dr_trace_op_t *op)
{
  prep->key = op->key;
  //prep->opcode = op->opcode;
  //prep->val_len = op->val_len;
  memcpy(prep->value, op->value, (size_t) VALUE_SIZE);
  //prep->m_id = ctx->m_id;
  //prep->sess_id = op->session_id;
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
  fifo_incr_capacity(dr_ctx->loc_w_rob_ptr);
}


//Inserts a LOCAL prep to the buffer
static inline void insert_prep_help(context_t *ctx, void* prep_ptr,
                                    void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;

  dr_prepare_t *prep = (dr_prepare_t *) prep_ptr;
  dr_trace_op_t *op = (dr_trace_op_t *) source;
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



static inline void dr_fill_trace_op(context_t *ctx,
                                    trace_t *trace_op,
                                    dr_trace_op_t *op,
                                    int working_session)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace_op, working_session, ctx->t_id);

  dr_check_op(op);

  if (ENABLE_ASSERTIONS) assert(op->opcode != NOP);
  bool is_update = op->opcode == KVS_OP_PUT;
  if (WRITE_RATIO >= 1000) assert(is_update);
  op->val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;

  op->session_id = (uint16_t) working_session;


  dr_ctx->stalled[working_session] =
    (is_update) || (USE_REMOTE_READS);

  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
    if (ENABLE_ASSERTIONS) assert(interface[ctx->t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[ctx->t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }

  if (ENABLE_ASSERTIONS == 1) {
    assert(WRITE_RATIO > 0 || is_update == 0);
    if (is_update) assert(op->val_len > 0);
  }
}


static inline void reset_dr_ctx_meta(context_t *ctx,
                                     w_rob_t *w_rob)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  uint32_t sess_id = w_rob->session_id;
  signal_completion_to_client(sess_id,
                              dr_ctx->index_to_req_array[sess_id],
                              ctx->t_id);
  dr_ctx->stalled[sess_id] = false;
  dr_ctx->all_sessions_stalled = false;
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