#ifndef _CQL_H_
#define _CQL_H_

#define DEFAULT_PORT "9042"

#define CQL_RESULT_SUCCESS 0x00
#define CQL_RESULT_CLIENT_ERROR 0x01
#define CQL_RESULT_SERVER_ERROR 0x02

#define CQL_CONSISTENCY_ANY 0x0000
#define CQL_CONSISTENCY_ONE 0x0001
#define CQL_CONSISTENCY_TWO 0x0002
#define CQL_CONSISTENCY_THREE 0x0003
#define CQL_CONSISTENCY_QUORUM 0x0004
#define CQL_CONSISTENCY_ALL 0x0005
#define CQL_CONSISTENCY_LOCAL_QUORUM 0x0006
#define CQL_CONSISTENCY_EACH_QUORUM 0x0007

#define CQL_RESULT_KIND_VOID 0x0001
#define CQL_RESULT_KIND_ROWS 0x0002
#define CQL_RESULT_KIND_SET_KEYSPACE 0x0003
#define CQL_RESULT_KIND_PREPARED 0x0004
#define CQL_RESULT_KIND_SCHEMA_CHANGE 0x0005

#define CQL_ERROR_SERVER_SIDE 0x0000
#define CQL_ERROR_PROTOCOL 0x000A
#define CQL_ERROR_BAD_CREDENTIALS 0x0100
#define CQL_ERROR_UNAVAILABLE 0x1000
#define CQL_ERROR_OVERLOADED 0x1001
#define CQL_ERROR_IS_BOOTSTRAPING 0x1002
#define CQL_ERROR_TRUNCATE 0x1003
#define CQL_ERROR_WRITE_TIMEOUT 0x1100
#define CQL_ERROR_READ_TIMEOUT 0x1200
#define CQL_ERROR_SYNTAX 0x2000
#define CQL_ERROR_UNAUTHORIZED 0x2100
#define CQL_ERROR_INVALID_QUERY 0x2200
#define CQL_ERROR_CONFIG_ISSUE 0x2300
#define CQL_ERROR_ALREADY_EXISTS 0x2400
#define CQL_ERROR_UNPREPARED 0x2500

#define CQL_COLUMN_TYPE_CUSTOM 0x0000
#define CQL_COLUMN_TYPE_ASCII 0x0001
#define CQL_COLUMN_TYPE_BIGINT 0x0002
#define CQL_COLUMN_TYPE_BLOB 0x0003
#define CQL_COLUMN_TYPE_BOOLEAN 0x0004
#define CQL_COLUMN_TYPE_COUNTER 0x0005
#define CQL_COLUMN_TYPE_DECIMAL 0x0006
#define CQL_COLUMN_TYPE_DOUBLE 0x0007
#define CQL_COLUMN_TYPE_FLOAT 0x0008
#define CQL_COLUMN_TYPE_INT 0x0009
#define CQL_COLUMN_TYPE_TEXT 0x000A
#define CQL_COLUMN_TYPE_TIMESTAMP 0x000B
#define CQL_COLUMN_TYPE_UUID 0x000C
#define CQL_COLUMN_TYPE_VARCHAR 0x000D
#define CQL_COLUMN_TYPE_VARINT 0x000E
#define CQL_COLUMN_TYPE_TIMEUUID 0x000F
#define CQL_COLUMN_TYPE_INET 0x0010
#define CQL_COLUMN_TYPE_LIST 0x0020
#define CQL_COLUMN_TYPE_MAP 0x0021
#define CQL_COLUMN_TYPE_SET 0x0022

typedef struct {
	int fd;
	char next_stream_id;
} cql_connection;

typedef struct {
	char *hostname;
	char *port;
	cql_connection **connections;
	unsigned short connections_count;
} cql_host;

typedef struct {
	cql_connection **connections;
	unsigned short connections_count;
} cql_session;

typedef struct {
	cql_host **hosts;
	unsigned short hosts_count;
	cql_session **sessions;
	unsigned short sessions_count;
} cql_cluster;

typedef struct {
	unsigned long kind;
	void *data;
} cql_result;

typedef struct {
	char *keyspace;
	char *table_name;
	char *column_name;
	short type;
	char *custom_type;
	short list_type;
	short value_type;
} cql_column;

typedef struct {
	unsigned long flags;
	unsigned long columns_count;
	char *global_keyspace;
	char *global_table_name;
	cql_column **columns;
} cql_metadata;

typedef struct {
	unsigned long length;
	void *value;
} cql_column_value;

typedef struct {
	cql_metadata *metadata;
	unsigned long rows_count;
	cql_column_value ***rows;
} cql_rows_result;

typedef struct {
	cql_connection *connection;
	char *id;
	unsigned short id_length;
	cql_metadata *metadata;
} cql_prepared_statement;

typedef struct {
	char *message;
} cql_client_error;

typedef struct {
	char opcode;
	unsigned long length;
	char* body;
} cql_frame;

typedef struct {
	char *key;
	char *value;
} cql_string_map;

typedef struct {
	char **values;
	unsigned short values_count;
} cql_string_list;

typedef struct {
	char *key;
	cql_string_list *value;
} cql_string_multimap;

typedef struct {
	char *change;
	char *keyspace;
	char *table;
} cql_schema_change;

typedef struct {
	unsigned long code;
	char *message;
	void *additional;
} cql_server_error;

typedef struct {
	unsigned short consistency;
	unsigned long required_nodes;
	unsigned long alive_nodes;
} cql_unavailable;

typedef struct {
	unsigned short consistency;
	unsigned long nodes_received;
	unsigned long nodes_required;
	char *write_type;
} cql_write_timeout;

typedef struct {
	unsigned short consistency;
	unsigned long nodes_received;
	unsigned long nodes_required;
	char data_present;
} cql_read_timeout;

typedef struct {
	char *keyspace;
	char *table;
} cql_already_exists;

int cql_cluster_create(cql_cluster **cluster);
void cql_cluster_destroy(cql_cluster *cluster);

int cql_host_create(cql_cluster *cluster, cql_host **host, char *host_port, void **result);
void cql_host_destroy(cql_host *host);

int cql_session_create(cql_cluster *cluster, cql_session **session, void **result);
int cql_session_query(cql_session *session, char *query, unsigned short consistency, void **result);
void cql_session_destroy(cql_session *session);

int cql_create_connection(cql_session *session, cql_host *host, cql_connection **connection, void **result);
int cql_connection_query(cql_connection *connection, char *query, unsigned short consistency, void **result);
void cql_connection_destroy(cql_connection *connection);

void cql_result_destroy(cql_result *result);
void cql_server_error_destroy(cql_server_error *error);

#endif
