#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdarg.h>
#include "cql.h"
#include "util.h"

#define OPCODE_ERROR 0x00
#define OPCODE_STARTUP 0x01
#define OPCODE_READY 0x02
#define OPCODE_AUTHENTICATE 0x03
#define OPCODE_CREDENTIALS 0x04
#define OPCODE_OPTIONS 0x05
#define OPCODE_SUPPORTED 0x06
#define OPCODE_QUERY 0x07
#define OPCODE_RESULT 0x08
#define OPCODE_PREPARE 0x09
#define OPCODE_EXECUTE 0x0A
#define OPCODE_REGISTER 0x0B
#define OPCODE_EVENT 0x0C

#define ROW_METADATA_FLAG_GLOBAL_TABLES 0x0001

#define PACK_BUFFER_BLOCK_SIZE 512

typedef struct {
	char *data;
	unsigned long written_len;
	unsigned long alloc_len;
} pack_buffer;

int cql_client_error_create(void **result, char *format, ...);
long cql_server_error_create(char *body, cql_server_error **error);
int cql_result_create(char *in, void **result);

int perform_startup_exchange(cql_connection *connection, void **result);

int round_trip_request_response(cql_connection *connection, cql_frame *request, cql_frame *response);
int send_frame(cql_connection *connection, cql_frame *request);
int read_frame(cql_connection *connection, cql_frame *response, char target_stream_id);

void pack_char(pack_buffer *buffer, char c);
void pack_long(pack_buffer *buffer, uint32_t i);
void pack_short(pack_buffer *buffer, uint16_t i);
void pack_string_map(pack_buffer *buffer, cql_string_map *map, unsigned short element_count);
void pack_string_common(pack_buffer *buffer, char *in, char long_string);
#define pack_string(buffer, in) pack_string_common(buffer, in, 0)
#define pack_long_string(buffer, in) pack_string_common(buffer, in, 1)
void write_pack_buffer(pack_buffer *buffer, char *bytes, unsigned long len);

int unpack_char(char *buffer, char *c);
int unpack_signed_short(char *buffer, short *n);
int unpack_unsigned_short(char *buffer, unsigned short *n);
int unpack_signed_long(char *buffer, long *n);
int unpack_unsigned_long(char *buffer, unsigned long *n);

int unpack_bytes_common(char *in, char **out, long *out_length, char long_bytes);
#define unpack_bytes(in, out, out_length) unpack_bytes_common(in, out, out_length, 1)
#define unpack_short_bytes(in, out, out_length) unpack_bytes_common(in, out, out_length, 0)
int unpack_string_common(char *in, char **string, char long_string);
#define unpack_string(in, out) unpack_string_common(in, out, 0)
#define unpack_signed_long_string(in, out) unpack_string_common(in, out, 1)
int unpack_string_list(char *in, cql_string_list **list);
int unpack_string_multimap(char *in, int in_len, cql_string_multimap **out);

int add_pointer(char ***list, int size, char *new_element);


int cql_cluster_create(cql_cluster **cluster) {
	*cluster = malloc(sizeof(cql_cluster));
	memset(*cluster, 0, sizeof(cql_cluster));

	return CQL_RESULT_SUCCESS;
}

void cql_cluster_destroy(cql_cluster *cluster) {
	int i;
	for(i = 0; i < cluster->sessions_count; i++) {
		cql_session_destroy(cluster->sessions[i]);
	}
	for(i = 0; i < cluster->hosts_count; i++) {
		cql_host_destroy(cluster->hosts[i]);
	}
	free(cluster);
}

int cql_host_create(cql_cluster *cluster, cql_host **host, char *host_port, void **result) {
	*host = malloc(sizeof(cql_host));
	memset(*host, 0, sizeof(cql_host));

	if(!parse_host_port(host_port, &((*host)->hostname), &((*host)->port))) {
		cql_host_destroy(*host);
		return cql_client_error_create(result, "Could not parse host and port from %s", host_port);
	}

	cluster->hosts_count = add_pointer((char***) &(cluster->hosts), cluster->hosts_count, (char *) *host);

	return CQL_RESULT_SUCCESS;
}

int cql_client_error_create(void **result, char *format, ...) {
	if(result && format) {

		cql_client_error *error = malloc(sizeof(cql_client_error));
		memset(error, 0, sizeof(cql_client_error));

		va_list args;
		va_start(args, format);
		int len = vsnprintf(NULL, 0, format, args);
		va_end(args);

		error->message = malloc(len + 1);
		va_start(args, format);
		vsnprintf(error->message, len + 1, format, args);
		va_end(args);

		*result = error;
	}

	return CQL_RESULT_CLIENT_ERROR;
}

void cql_client_error_destroy(cql_client_error *error) {
	if(error->message)
		free(error->message);
	free(error);
}

int add_pointer(char ***list, int size, char *new_element) {
	char **new_list = malloc(sizeof(void *) * (size + 1));
	int i;
	for(i = 0; i < size; i++) {
		new_list[i] = (*list)[i];
	}
	new_list[size] = new_element;

	if(*list)
		free(*list);
	*list = new_list;

	return size + 1;
}

void cql_host_destroy(cql_host *host) {
	int i;
	for(i = 0; i < host->connections_count; i++) {
		cql_connection_destroy(host->connections[i]);
	}

	if(host->hostname)
		free(host->hostname);
	if(host->port)
		free(host->port);

	free(host);
}

int cql_session_create(cql_cluster *cluster, cql_session **session, void **result) {
	*session = malloc(sizeof(cql_session));
	memset(*session, 0, sizeof(cql_session));

	int i;
	for(i = 0; i < cluster->hosts_count; i++) {
		cql_connection *connection;
		int res = cql_create_connection(*session, cluster->hosts[i], &connection, result);
		if(res != CQL_RESULT_SUCCESS) {
			cql_session_destroy(*session);
			return res;
		}

		add_pointer((char***) &((*session)->connections), (*session)->connections_count, (char*) connection);
	}

	return CQL_RESULT_SUCCESS;
}

int cql_create_connection(cql_session *session, cql_host *host, cql_connection **connection, void **result) {
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = PF_INET;

	struct addrinfo *info;
	int res = getaddrinfo(host->hostname, host->port, &hints, &info);
	if(res != 0)
		return cql_client_error_create(result, "Could not getaddrinfo() on %s:%s", host->hostname, host->port);

	struct addrinfo *ci;
	int sfd;
	for(ci = info; ci != NULL; ci = ci->ai_next) {
		sfd = socket(ci->ai_family, ci->ai_socktype, ci->ai_protocol);
		if(sfd == -1)
			continue;

		if(connect(sfd, ci->ai_addr, ci->ai_addrlen) == 0)
			break;
	}

	freeaddrinfo(info);
	if(sfd == -1)
		return cql_client_error_create(result, "Could not connect socket to %s:%s", host->hostname, host->port);

	*connection = malloc(sizeof(cql_connection));
	memset(*connection, 0, sizeof(cql_connection));

	(*connection)->fd = sfd;
	(*connection)->next_stream_id = 1;

	res = perform_startup_exchange(*connection, result);
	if(res != CQL_RESULT_SUCCESS) {
		cql_connection_destroy(*connection);
		return res;
	}

	return CQL_RESULT_SUCCESS;
}

int cql_connection_query(cql_connection *connection, char *query, unsigned short consistency, void **result) {
	cql_frame request;
	memset(&request, 0, sizeof(cql_frame));
	request.opcode = OPCODE_QUERY;

	pack_buffer buffer;
	memset(&buffer, 0, sizeof(pack_buffer));
	pack_long_string(&buffer, query);
	pack_short(&buffer, consistency);
	request.length = buffer.written_len;
	request.body = buffer.data;

	cql_frame response;
	int round_trip_res = round_trip_request_response(connection, &request, &response);
	free(request.body);
	if(!round_trip_res)
		return cql_client_error_create(result, "Unable to round trip frames");

	switch(response.opcode) {
	case OPCODE_RESULT:
		return cql_result_create(response.body, result);
	case OPCODE_ERROR:
		cql_server_error_create(response.body, (cql_server_error**) result);
		return CQL_RESULT_SERVER_ERROR;
	}

	return unexpected_opcode(result, response.opcode);
}

int cql_connection_options(cql_connection *connection, void **result) {
	cql_frame request;
	memset(&request, 0, sizeof(request));
	request.opcode = OPCODE_OPTIONS;

	cql_frame response;
	if(round_trip_request_response(connection, &request, &response) == 0)
		return cql_client_error_create(result, "Unable to round trip frames");

	int result_code;
	if(response.opcode == OPCODE_SUPPORTED) {
		cql_string_multimap *string_multimap;
		unpack_string_multimap(response.body, response.length, &string_multimap);

		result_code = CQL_RESULT_SUCCESS;
		*result = string_multimap;
	} else {
		result_code = unexpected_opcode(result, response.opcode);
	}

	free(response.body);

	return result_code;
}

int cql_connection_prepare(cql_connection *connection, char *query, void **result) {
	cql_frame request;
	request.opcode = OPCODE_PREPARE;
	pack_buffer buffer;
	memset(&buffer, 0, sizeof(pack_buffer));
	pack_long_string(&buffer, query);
	request.length = buffer.written_len;
	request.body = buffer.data;

	cql_frame response;
	int res = round_trip_request_response(connection, &request, &response);
	free(request.body);
	if(res == 0)
		return cql_client_error_create(result, "Unable to round trip frames");

	int result_code;
	if(response.opcode == OPCODE_RESULT) {
		cql_prepared_statement *prepared_statement = malloc(sizeof(cql_prepared_statement));
		memset(prepared_statement, 0, sizeof(cql_prepared_statement));

		char *body_offset = response.body;
    long id_length;
		body_offset += unpack_bytes(body_offset, &(prepared_statement->id), &id_length);
    prepared_statement->id_length = id_length;
		body_offset += unpack_cql_metadata(body_offset, &(prepared_statement->metadata));

		result_code = CQL_RESULT_SUCCESS;
		*result = prepared_statement;
	} else {
		return unexpected_opcode(result, response.opcode);
	}

	free(response.body);

	return result_code;
}

int cql_connection_execute(cql_prepared_statement *prepared_statement, char **values, unsigned short values_count, unsigned short consistency) {
	pack_buffer buffer;
	memset(&buffer, 0, sizeof(pack_buffer));

	pack_short_bytes(&buffer, prepared_statement->id, prepared_statement->id_length);

	cql_frame request;
	request.opcode = OPCODE_EXECUTE;
	request.body = buffer.data;
	request.length = buffer.written_len;

	free(buffer.data);
}

int cql_session_query(cql_session *session, char *query, unsigned short consistency, void **result) {
	// TODO Need something like the Java load balancing policy. Until then,
	// just pick the first connection
	cql_connection *connection = session->connections[0];

	return cql_connection_query(connection, query, consistency, result);
}

void cql_session_destroy(cql_session *session) {
	int i;
	for(i = 0; i < session->connections_count; i++) {
		cql_connection_destroy(session->connections[i]);
	}
	free(session);
}

void cql_connection_destroy(cql_connection *connection) {
	if(connection->fd)
		close(connection->fd);

	free(connection);
}

int parse_host_port(char *host_port, char **host, char **port) {
	char *token = strtok(host_port, ":");
	if(!token)
		return 0;
	*host = malloc(strlen(token) + 1);
	strcpy(*host, token);

	token = strtok(NULL, ":");
	if(!token) {
		token = DEFAULT_PORT;
	} else {
		if(strtok(NULL, ":")) {
			free(*host);
			*host = NULL;
			return 0;
		}
	}

	*port = malloc(strlen(token) + 1);
	strcpy(*port, token);

	return 1;
}

int perform_startup_exchange(cql_connection *connection, void **result) {
	cql_string_map body_map;
	body_map.key = "CQL_VERSION";
	body_map.value = "3.0.0";

	cql_frame request;
	request.opcode = OPCODE_STARTUP;
	pack_buffer buffer;
	memset(&buffer, 0, sizeof(pack_buffer));
	pack_string_map(&buffer, &body_map, 1);
	request.body = buffer.data;
	request.length = buffer.written_len;

	int stream_id = send_frame(connection, &request);
	free(request.body);
	if(stream_id == 0)
		return 0;

	cql_frame response;
	if(read_frame(connection, &response, stream_id) == 0)
		return 0;

	switch(response.opcode) {
	case OPCODE_READY:
		return CQL_RESULT_SUCCESS;
	case OPCODE_ERROR:
		if(result)
			cql_server_error_create(response.body, (cql_server_error**) result);
		return CQL_RESULT_SERVER_ERROR;
	}

	return unexpected_opcode(result, response.opcode);
}

int unexpected_opcode(void **result, int opcode) {
	return cql_client_error_create(result, "Got unexpected opcode %d", opcode);
}

int unpack_cql_metadata(char *in, cql_metadata **out) {
	cql_metadata *metadata = malloc(sizeof(cql_metadata));
	memset(metadata, 0, sizeof(cql_metadata));

	char *in_offset = in;
	in_offset += unpack_unsigned_long(in_offset, &(metadata->flags));
	in_offset += unpack_unsigned_long(in_offset, &(metadata->columns_count));
	if(metadata->flags & ROW_METADATA_FLAG_GLOBAL_TABLES) {
		in_offset += unpack_string(in_offset, &(metadata->global_keyspace));
		in_offset += unpack_string(in_offset, &(metadata->global_table_name));
	}

	if(metadata->columns_count > 0) {
		int columns_size = sizeof(cql_column*) * metadata->columns_count;
		metadata->columns = malloc(columns_size);
		memset(metadata->columns, 0, columns_size);

		int i;
		for(i = 0; i < metadata->columns_count; i++) {
			cql_column *column = metadata->columns[i] = malloc(sizeof(cql_column));
			memset(column, 0, sizeof(cql_column));

			if(!(metadata->flags & ROW_METADATA_FLAG_GLOBAL_TABLES)) {
				in_offset += unpack_string(in_offset, &(column->keyspace));
				in_offset += unpack_string(in_offset, &(column->table_name));
			}

			in_offset += unpack_string(in_offset, &(column->column_name));
			in_offset += unpack_unsigned_short(in_offset, &(column->type));
			switch(column->type) {
			case CQL_COLUMN_TYPE_CUSTOM:
				in_offset += unpack_string(in_offset, &(column->custom_type));
				break;
			case CQL_COLUMN_TYPE_LIST:
			case CQL_COLUMN_TYPE_SET:
			case CQL_COLUMN_TYPE_MAP:
				in_offset += unpack_unsigned_short(in_offset, &(column->list_type));
				if(column->type == CQL_COLUMN_TYPE_MAP)
					in_offset += unpack_unsigned_short(in_offset, &(column->value_type));
				break;
			}
		}
	}

	*out = metadata;
	return in_offset - in;
}

int cql_result_create(char *in, void **res) {
	cql_result *result = malloc(sizeof(cql_result));
	memset(result, 0, sizeof(cql_result));

	char *in_offset = in;
	in_offset += unpack_signed_long(in_offset, &(result->kind));

	switch(result->kind) {
	case CQL_RESULT_KIND_VOID:
		// Noop cause there is no data
		result->data = NULL;
		break;
	case CQL_RESULT_KIND_ROWS:
		{
			cql_rows_result *rows_result = malloc(sizeof(cql_rows_result));
			memset(rows_result, 0, sizeof(cql_rows_result));

			in_offset += unpack_cql_metadata(in_offset, &(rows_result->metadata));
			cql_metadata *metadata = rows_result->metadata;

			in_offset += unpack_signed_long(in_offset, &(rows_result->rows_count));
			int rows_size = sizeof(cql_column_value**) * rows_result->rows_count;
			rows_result->rows = malloc(rows_size);
			memset(rows_result->rows, 0, rows_size);

			int row_index;
			for(row_index = 0; row_index < rows_result->rows_count; row_index++) {
				int row_size = sizeof(cql_column_value*) * metadata->columns_count;
				cql_column_value **row = malloc(row_size);
				memset(row, 0, row_size);

				int column_index, row_allocated = 0, row_offset = 0;
				for(column_index = 0; column_index < metadata->columns_count; column_index++) {
					cql_column *column_metadata = metadata->columns[column_index];

					char *bytes = NULL;
					long bytes_length;
					in_offset += unpack_bytes(in_offset, &bytes, &bytes_length);

					if(bytes_length < 0)
						continue;

					cql_column_value *column_value = malloc(sizeof(cql_column_value));
					memset(column_value, 0, sizeof(cql_column_value));
					if(bytes_length > 0) {
						switch(column_metadata->type) {
						case CQL_COLUMN_TYPE_ASCII:
						case CQL_COLUMN_TYPE_TEXT:
						case CQL_COLUMN_TYPE_VARCHAR:
							column_value->length = bytes_length + 1;
							column_value->value = malloc(column_value->length);
							strncpy(column_value->value, bytes, bytes_length);
							((char *) column_value->value)[bytes_length] = 0;
							break;
						case CQL_COLUMN_TYPE_BOOLEAN:
							if(bytes_length != sizeof(char))
								printf("BAD BOOLEAN SIZE %lu\n", bytes_length); // Handle error
							column_value->length = bytes_length;
							column_value->value = malloc(column_value->length);
							memcpy(column_value->value, bytes, column_value->length);
							break;
						case CQL_COLUMN_TYPE_INT:
							if(bytes_length != 4)
								printf("BAD 32 INT SIZE %lu\n", bytes_length); // Handle error
							column_value->length = bytes_length;
							column_value->value = malloc(column_value->length);
							unpack_signed_long(bytes, (long *) &column_value->value);
							break;
						case CQL_COLUMN_TYPE_BIGINT:
						case CQL_COLUMN_TYPE_COUNTER:
							// TODO Implement this later because shifting bits to host endianness is supposedly painful
							// if(bytes_length != 8)
							//	printf("BAD 64 BIT LONG SIZE"); // Handle error
							// column = malloc(sizeof(int64_t));
							// (column, bytes, bytes_length);
							// break;
						case CQL_COLUMN_TYPE_DECIMAL:
							// Need an arbitrary precision math lib, maybe
							// break;
						case CQL_COLUMN_TYPE_DOUBLE:
							// 64 bit double
							// break;
						case CQL_COLUMN_TYPE_FLOAT:
							// 32 bit float
							// break;
						case CQL_COLUMN_TYPE_TIMESTAMP:
							// YYYY-MM-DD
							// break;
						case CQL_COLUMN_TYPE_UUID:
						case CQL_COLUMN_TYPE_TIMEUUID:
							// 128 bit
							// break;
						case CQL_COLUMN_TYPE_VARINT:
							// Need an arbitrary precision math lib, maybe
							// break;
						case CQL_COLUMN_TYPE_INET:
							// Char array, one byte for each octet
							// break;
						case CQL_COLUMN_TYPE_LIST:
							// Need to look at Java source
							// break;
						case CQL_COLUMN_TYPE_MAP:
							// Need to look at Java source
							// break;
						case CQL_COLUMN_TYPE_SET:
							// Need to look at Java source
							// break;
						case CQL_COLUMN_TYPE_CUSTOM:
						case CQL_COLUMN_TYPE_BLOB:
							column_value->length = bytes_length;
							column_value->value = malloc(bytes_length);
							memcpy(column_value->value, bytes, bytes_length);
							break;
						}
						free(bytes);
					}

					row[column_index] = column_value;
				}
				rows_result->rows[row_index] = row;
			}

			result->data = rows_result;
		}
		break;
	case CQL_RESULT_KIND_SET_KEYSPACE:
		in_offset += unpack_string(in_offset, (char **) &(result->data));
		break;
	case CQL_RESULT_KIND_PREPARED:
		{
			cql_prepared_statement *prepared_statement = malloc(sizeof(cql_prepared_statement));
			memset(prepared_statement, 0, sizeof(cql_prepared_statement));

			in_offset += unpack_signed_long_bytes(in_offset, &(prepared_statement->id));
			in_offset += unpack_metadata(in_offset, &(prepared_statement->metadata));
		}
		break;
	case CQL_RESULT_KIND_SCHEMA_CHANGE:
		{
			cql_schema_change *sc = malloc(sizeof(cql_schema_change));
			in_offset += unpack_string(in_offset, &(sc->change));
			in_offset += unpack_string(in_offset, &(sc->keyspace));
			in_offset += unpack_string(in_offset, &(sc->table));
			result->data = sc;
		}
		break;
	default:
		// TODO Need to look at this
		// cql_result_destroy(result);
		return cql_client_error_create((void**) result, "Unknown result kind %lu", result->kind);
	}

	*res = (void*) result;

	return CQL_RESULT_SUCCESS;
}

void cql_server_error_destroy(cql_server_error *error) {
	if(error->message)
		free(error->message);

	// TODO Free the additional member of the struct
	free(error);
}

void cql_result_destroy(cql_result *result) {
	if(result->data) {
		switch(result->kind) {
		case CQL_RESULT_KIND_ROWS:
			{
				cql_rows_result *rows_result = result->data;
				cql_metadata *metadata = rows_result->metadata;
				if(metadata && rows_result->rows) {
					int rows_index;
					for(rows_index = 0; rows_index < rows_result->rows_count; rows_index++) {
						cql_column_value **row = rows_result->rows[rows_index];
						if(row) {
							int column_index;
							for(column_index = 0; column_index < metadata->columns_count; column_index++) {
								cql_column_value *column_value = row[column_index];
								if(column_value) {
									// TODO This segfaults. Need to figure it out later to prevent memory leak.
									// if(column_value->value)
									//	 free(column_value->value);

									free(column_value);
								}
							}
							free(row);
						}
					}
					free(rows_result->rows);
				}

				if(metadata) {
					if(metadata->columns) {
						cql_column **columns = metadata->columns;
						int i;
						for(i = 0; i < metadata->columns_count; i++) {
							cql_column *column = metadata->columns[i];
							if(column->keyspace)
								free(column->keyspace);
							if(column->table_name)
								free(column->table_name);
							if(column->column_name)
								free(column->column_name);
						}
						free(metadata->columns);
					}

					if(metadata->global_keyspace)
						free(metadata->global_keyspace);
					if(metadata->global_table_name)
						free(metadata->global_table_name);

					free(metadata);
				}

				free(result->data);
			}
			break;
		case CQL_RESULT_KIND_SET_KEYSPACE:
			free(result->data);
			break;
		case CQL_RESULT_KIND_SCHEMA_CHANGE:
			{
				cql_schema_change *sc = (cql_schema_change*) result->data;
				free(sc->change);
				free(sc->keyspace);
				free(sc->table);
			}
			break;
		}
	}

	free(result);
}

long cql_server_error_create(char *body, cql_server_error **error) {
	cql_server_error *err = malloc(sizeof(cql_server_error));
	char *body_offset = body;
	body_offset += unpack_signed_long(body_offset, &(err->code));
	body_offset += unpack_string(body_offset, &err->message);

	switch(err->code) {
	case CQL_ERROR_UNAVAILABLE:
		{
			cql_unavailable *unavailable = malloc(sizeof(cql_unavailable));
			body_offset += unpack_unsigned_short(body_offset, &(unavailable->consistency));
			body_offset += unpack_signed_long(body_offset, &(unavailable->required_nodes));
			body_offset += unpack_signed_long(body_offset, &(unavailable->alive_nodes));
			err->additional = unavailable;
		}
		break;
	case CQL_ERROR_WRITE_TIMEOUT:
		{
			cql_write_timeout *write_timeout = malloc(sizeof(cql_write_timeout)); // Memory leak
			body_offset += unpack_unsigned_short(body_offset, &(write_timeout->consistency));
			body_offset += unpack_signed_long(body_offset, &(write_timeout->nodes_received));
			body_offset += unpack_signed_long(body_offset, &(write_timeout->nodes_required));
			body_offset += unpack_string(body_offset, &write_timeout->write_type); // Memory leak

			err->additional = write_timeout;
		}
		break;
	case CQL_ERROR_READ_TIMEOUT:
		{
			cql_read_timeout *read_timeout = malloc(sizeof(cql_read_timeout)); // Memory leak
			body_offset += unpack_unsigned_short(body_offset, &(read_timeout->consistency));
			body_offset += unpack_signed_long(body_offset, &(read_timeout->nodes_received));
			body_offset += unpack_signed_long(body_offset, &(read_timeout->nodes_required));

			memcpy(&read_timeout->data_present, body_offset, 1);
			body_offset++;

			err->additional = read_timeout;
		}
		break;
	case CQL_ERROR_ALREADY_EXISTS:
		{
			cql_already_exists *ae = malloc(sizeof(cql_already_exists));
			body_offset += unpack_string(body_offset, &(ae->keyspace));
			body_offset += unpack_string(body_offset, &(ae->table));
			err->additional = (void *) ae;
		}
		break;
	}

	*error = err;

	return (*error)->code;
}

int round_trip_request_response(cql_connection *connection, cql_frame *request, cql_frame *response) {
	int stream_id;
	if(!(stream_id = send_frame(connection, request)))
		return 0;

	return read_frame(connection, response, stream_id);
}

int send_frame(cql_connection *connection, cql_frame *request) {
	int stream_id = connection->next_stream_id++;

	pack_buffer buffer;
	memset(&buffer, 0, sizeof(pack_buffer));

	pack_char(&buffer, 0x01);
	pack_char(&buffer, 0x01);
	pack_char(&buffer, stream_id);
	pack_char(&buffer, request->opcode);

	pack_long(&buffer, request->length);
	write_pack_buffer(&buffer, request->body, request->length);

	int wrote_all = write(connection->fd, buffer.data, buffer.written_len) == buffer.written_len;
	free(buffer.data);

	return !wrote_all ? 0 : stream_id;
}

// TODO This function needs to be replaced by epoll/kqueue or something
int read_frame(cql_connection *connection, cql_frame *response, char target_stream_id) {
	const int BLOCK_BUFFER_SIZE = 1024;

	memset(response, 0, sizeof(cql_frame));

	char block_buffer[BLOCK_BUFFER_SIZE];
	char *buffer = NULL;
	unsigned long bytes_read = 0;
	int readres;
	// TODO This loop could be infinite. Need poll() with timeout.
	for(;;) {
		readres = read(connection->fd, block_buffer, BLOCK_BUFFER_SIZE);
		if(readres > 0) {
			buffer = realloc(buffer, bytes_read + readres);
			memcpy(buffer + bytes_read, block_buffer, readres);
			bytes_read += readres;
		}

		if(readres != 0 && readres != BLOCK_BUFFER_SIZE)
			break;
	}

	if(readres == -1)
		return 0;

  char *buffer_offset = buffer;
  int response_found = 0;
	while((buffer_offset - buffer) < bytes_read) {
		if(bytes_read - (buffer_offset - buffer) < 8)
			break; // TODO Client error saying not enough room for another frame

		char version, flags, stream, opcode;
		long length;
    buffer_offset += unpack_char(buffer_offset, &version);
    buffer_offset += unpack_char(buffer_offset, &flags);
    buffer_offset += unpack_char(buffer_offset, &stream);
    buffer_offset += unpack_char(buffer_offset, &opcode);
		buffer_offset += unpack_unsigned_long(buffer_offset, &length);

		if((buffer_offset - buffer) + length > bytes_read)
			break; // TODO Client error saying not enough room for frame body

		char *body = NULL;
		if(length > 0) {
			body = malloc(length);
			memcpy(body, buffer_offset, length);
		}

		// TODO This is lame because we could read and ignore frames
		if(stream == target_stream_id) {
			response->opcode = opcode;
			response->length = length;
			response->body = body;
      response_found = 1;
      break;
		}

    free(body);
	}
	free(buffer);

	return response_found;
}

void pack_string_map(pack_buffer *buffer, cql_string_map *map, unsigned short element_count) {
	pack_short(buffer, element_count);

	int i;
	for(i = 0; i < element_count; i++) {
		pack_string(buffer, map[i].key);
		pack_string(buffer, map[i].value);
	}
}

void pack_string_common(pack_buffer *buffer, char *in, char long_string) {
	unsigned long len = strlen(in);
	char int_size = long_string ? 4 : 2;

	if(long_string)
		pack_long(buffer, len);
	else
		pack_short(buffer, (unsigned short) len);

	if(len > 0)
		write_pack_buffer(buffer, in, len);
}

void pack_char(pack_buffer *buffer, char c) {
	write_pack_buffer(buffer, &c, sizeof(char));
}

void pack_long(pack_buffer *buffer, uint32_t i) {
	uint32_t n = htonl(i);
	write_pack_buffer(buffer, (char *) &n, sizeof(n));
}

void pack_short(pack_buffer *buffer, uint16_t i) {
	uint16_t n = htons(i);
	write_pack_buffer(buffer, (char *) &n, sizeof(n));
}

void write_pack_buffer(pack_buffer *buffer, char *bytes, unsigned long len) {
	if(buffer->written_len + len > buffer->alloc_len) {
		buffer->alloc_len += PACK_BUFFER_BLOCK_SIZE;
		buffer->data = realloc(buffer->data, buffer->alloc_len);
	}

	memcpy(buffer->data + buffer->written_len, bytes, len);
	buffer->written_len += len;
}

int unpack_string_common(char *in, char **out, char long_string) {
	char *in_offset = in;
	unsigned long len = 0;
	in_offset += long_string ? unpack_signed_long(in, &len) : unpack_unsigned_short(in, (unsigned short*) &len);
	char int_size = long_string ? 4 : 2;
	*out = malloc(len + 1);
	if(len > 0)
		memcpy(*out, in + int_size, len);
	memset(*out + len, 0, 1);

	return int_size + len;
}

int unpack_bytes_common(char *in, char **out, long *out_length, char long_bytes) {
	char *in_offset = in;
	if(long_bytes) {
		in_offset += unpack_signed_long(in_offset, out_length);
	} else {
    short tmp_out_length;
		in_offset += unpack_signed_short(in_offset, &tmp_out_length);
    *out_length = tmp_out_length;
  }

	long len = *out_length;
	if(len > 0) {
		*out = malloc(len);
		memcpy(*out, in_offset, len);
		in_offset += len;
	} else {
		*out = NULL;
	}

	return in_offset - in;
}

int unpack_string_multimap(char *in, int in_len, cql_string_multimap **out) {
	char *in_offset = in;
	unsigned short size = 0;
	in_offset += unpack_unsigned_short(in_offset, &size);

	int multimap_size = sizeof(cql_string_multimap) * size;
	cql_string_multimap *multimap = malloc(multimap_size);
	memset(multimap, 0, multimap_size);

	int i;
	for(i = 0; i < size; i++) {
		in_offset += unpack_string(in_offset, &multimap[i].key);
		in_offset += unpack_string_list(in_offset, &multimap[i].value);
	}

	*out = multimap;

	return in_offset - in;
}

int unpack_string_list(char *in, cql_string_list **list) {
	*list = malloc(sizeof(cql_string_list));
	memset(*list, 0, sizeof(cql_string_list));

	char *in_offset = in;
	in_offset += unpack_unsigned_short(in_offset, &((*list)->values_count));

	(*list)->values = malloc(sizeof(char*) * (*list)->values_count);
	memset((*list)->values, 0, (*list)->values_count);

	int i;
	for(i = 0; i < (*list)->values_count; i++) {
		in_offset += unpack_string(in_offset, &(*list)->values[i]);
	}

	return in_offset - in;
}

int unpack_signed_short(char *buffer, short *n) {
  memcpy(n, buffer, 2);
  *n = (int16_t) ntohs(*n);
  return 2;
}

int unpack_unsigned_short(char *buffer, unsigned short *n) {
  memcpy(n, buffer, 2);
  *n = ntohs(*n);
  return 2;
}

int unpack_signed_long(char *buffer, long *n) {
  memcpy(n, buffer, 4);
  *n = (int32_t) ntohl(*n);
  return 4;
}

int unpack_unsigned_long(char *buffer, unsigned long *n) {
  memcpy(n, buffer, 4);
  *n = ntohl(*n);
  return 4;
}

int unpack_char(char *buffer, char *c) {
  *c = *buffer;
  return 1;
}
