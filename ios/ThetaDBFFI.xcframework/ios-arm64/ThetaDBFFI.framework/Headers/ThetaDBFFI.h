#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum FFICallCode {
  FFICallSuccess = 0,
  FFICallPanic,
  FFICallErrIO,
  FFICallErrInputInvalid,
  FFICallErrFileUnexpected,
  FFICallErrDBCorrupted,
} FFICallCode;

typedef struct FFIBytes {
  void *ptr;
  uint32_t length;
  uint32_t capacity;
} FFIBytes;

typedef struct FFICallState {
  enum FFICallCode code;
  struct FFIBytes err_desc;
} FFICallState;

typedef struct FFIBytesRef {
  const void *ptr;
  uint32_t length;
} FFIBytesRef;

typedef struct ThetaDBOptions {
  uint32_t page_size;
  uint8_t force_sync;
  uint64_t mempool_capacity;
} ThetaDBOptions;

void thetadb_bytes_dealloc(struct FFIBytes bytes, struct FFICallState *call_state);

void *thetadb_new(struct FFIBytesRef path,
                  struct ThetaDBOptions options,
                  struct FFICallState *call_state);

void thetadb_dealloc(void *db, struct FFICallState *call_state);

uint8_t thetadb_contains(const void *db, struct FFIBytesRef key, struct FFICallState *call_state);

struct FFIBytes thetadb_get(const void *db,
                            struct FFIBytesRef key,
                            struct FFICallState *call_state);

void thetadb_put(const void *db,
                 struct FFIBytesRef key,
                 struct FFIBytesRef value,
                 struct FFICallState *call_state);

void thetadb_delete(const void *db, struct FFIBytesRef key, struct FFICallState *call_state);

void *thetadb_begin_tx(const void *db, struct FFICallState *call_state);

void thetadb_tx_dealloc(void *tx, struct FFICallState *call_state);

uint8_t thetadb_tx_contains(const void *tx,
                            struct FFIBytesRef key,
                            struct FFICallState *call_state);

struct FFIBytes thetadb_tx_get(const void *tx,
                               struct FFIBytesRef key,
                               struct FFICallState *call_state);

void *thetadb_begin_tx_mut(const void *db, struct FFICallState *call_state);

void thetadb_tx_mut_dealloc(void *tx, struct FFICallState *call_state);

uint8_t thetadb_tx_mut_contains(const void *tx,
                                struct FFIBytesRef key,
                                struct FFICallState *call_state);

struct FFIBytes thetadb_tx_mut_get(const void *tx,
                                   struct FFIBytesRef key,
                                   struct FFICallState *call_state);

void thetadb_tx_mut_put(void *tx,
                        struct FFIBytesRef key,
                        struct FFIBytesRef value,
                        struct FFICallState *call_state);

void thetadb_tx_mut_delete(void *tx, struct FFIBytesRef key, struct FFICallState *call_state);

void thetadb_tx_mut_commit(void *tx, struct FFICallState *call_state);

void *thetadb_first_cursor(const void *db, struct FFICallState *call_state);

void *thetadb_last_cursor(const void *db, struct FFICallState *call_state);

void *thetadb_cursor_from_key(const void *db,
                              struct FFIBytesRef key,
                              struct FFICallState *call_state);

void thetadb_cursor_dealloc(const void *cursor, struct FFICallState *call_state);

uint8_t thetadb_cursor_next(void *cursor, struct FFICallState *call_state);

uint8_t thetadb_cursor_prev(void *cursor, struct FFICallState *call_state);

struct FFIBytes thetadb_cursor_key(void *cursor, struct FFICallState *call_state);

struct FFIBytes thetadb_cursor_value(void *cursor, struct FFICallState *call_state);

void thetadb_cursor_key_value(void *cursor,
                              struct FFIBytes *key,
                              struct FFIBytes *value,
                              struct FFICallState *call_state);
