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

int cql_client_error_create(void **result, const char *format, ...);
int cql_result_create(cql_frame *frame, void **result);
long cql_server_error_create(cql_frame *frame, cql_server_error **error);

int perform_startup_exchange(cql_connection *connection, void **result);

int round_trip_request_response(cql_connection *connection, cql_frame *request, cql_frame **response);
int send_frame(cql_connection *connection, cql_frame *request);
int read_frame(cql_connection *connection, cql_frame **response, char target_stream_id);

uint32_t pack_long(char *buffer, uint32_t i);
uint16_t pack_short(char *buffer, uint16_t i);
int pack_string_map(cql_string_map *map, unsigned short element_count, char** buffer);
int pack_string_common(char *in, char **out, char long_string);
#define pack_string(in, out) pack_string_common(in, out, 0)
#define pack_long_string(in, out) pack_string_common(in, out, 1)

int unpack_long(char *buffer, long *n);
int unpack_short(char *buffer, short *n);
int unpack_bytes(char *in, char **out, long *out_length);
int unpack_string_common(char *in, char **string, char long_string);
#define unpack_string(in, out) unpack_string_common(in, out, 0)
#define unpack_long_string(in, out) unpack_string_common(in, out, 1)
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

int cql_client_error_create(void **result, const char *format, ...) {
	if(result && format) {
		cql_client_error *error = malloc(sizeof(cql_client_error));
		memset(error, 0, sizeof(cql_client_error));

		va_list args;
		va_start(args, format);

		int len = vsnprintf(NULL, 0, format, args);
		error->message = malloc(len + 1);
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
	char *serialized_query;
	int serialized_query_len = pack_long_string(query, &serialized_query);

	cql_frame request;
	request.opcode = OPCODE_QUERY;
	request.length = serialized_query_len + 2;
	request.body = malloc(request.length);
	memcpy(request.body, serialized_query, serialized_query_len);
	pack_short(request.body + serialized_query_len, consistency);

	cql_frame *response;
	int round_trip_res = round_trip_request_response(connection, &request, &response);
	free(request.body);
	if(!round_trip_res)
		return cql_client_error_create(result, "Unable to round trip frames");

	switch(response->opcode) {
	case OPCODE_RESULT:
		return cql_result_create(response, result);
	case OPCODE_ERROR:
		cql_server_error_create(response, (cql_server_error**) result);
		return CQL_RESULT_SERVER_ERROR;
	}

	return unexpected_opcode(result, response->opcode);
}

int cql_connection_options(cql_connection *connection, void **result) {
	cql_frame request;
	request.opcode = OPCODE_OPTIONS;
	request.length = 0;
	request.body = 0;

	cql_frame *response;
	if(round_trip_request_response(connection, &request, (cql_frame**) response) == 0)
		return cql_client_error_create(result, "Unable to round trip frames");

	switch(response->opcode) {
	case OPCODE_SUPPORTED:
		unpack_string_multimap(request.body, request.length, &response);
		return CQL_RESULT_SUCCESS;
	}

	return unexpected_opcode(result, response->opcode);
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
	request.length = pack_string_map(&body_map, 1, &request.body);
	int stream_id = send_frame(connection, &request);
	free(request.body);
	if(stream_id == 0)
		return 0;

	cql_frame *response;
	if(read_frame(connection, &response, stream_id) == 0)
		return 0;

	switch(response->opcode) {
	case OPCODE_READY:
		return CQL_RESULT_SUCCESS;
	case OPCODE_ERROR:
		if(result)
			cql_server_error_create(response, (cql_server_error**) result);
		return CQL_RESULT_SERVER_ERROR;
	}

	return unexpected_opcode(result, response->opcode);
}

int unexpected_opcode(void **result, int opcode) {
	return cql_client_error_create(result, "Got unexpected opcode %d", opcode);
}

int cql_result_create(cql_frame *frame, void **result) {
	cql_result *res = malloc(sizeof(cql_result));
	memset(res, 0, sizeof(cql_result));

	char *body_offset = frame->body;
	body_offset += unpack_long(body_offset, &(res->kind));

	switch(res->kind) {
	case CQL_RESULT_KIND_VOID:
		// Noop cause there is no data
		res->data = NULL;
		break;
	case CQL_RESULT_KIND_ROWS:
		{
			cql_rows_result *rows_result = malloc(sizeof(cql_rows_result));
			memset(rows_result, 0, sizeof(cql_rows_result));

			cql_rows_metadata *metadata = malloc(sizeof(cql_rows_metadata));
			memset(metadata, 0, sizeof(cql_rows_metadata));

			body_offset += unpack_long(body_offset, &(metadata->flags));
			body_offset += unpack_long(body_offset, &(metadata->columns_count));
			if(metadata->flags & ROW_METADATA_FLAG_GLOBAL_TABLES) {
				body_offset += unpack_string(body_offset, &(metadata->global_keyspace));
				body_offset += unpack_string(body_offset, &(metadata->global_table_name));
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
						body_offset += unpack_string(body_offset, &(column->keyspace));
						body_offset += unpack_string(body_offset, &(column->table_name));
					}

					body_offset += unpack_string(body_offset, &(column->column_name));
					body_offset += unpack_short(body_offset, &(column->type));
					switch(column->type) {
					case CQL_COLUMN_TYPE_CUSTOM:
						body_offset += unpack_string(body_offset, &(column->custom_type));
						break;
					case CQL_COLUMN_TYPE_LIST:
					case CQL_COLUMN_TYPE_SET:
					case CQL_COLUMN_TYPE_MAP:
						body_offset += unpack_short(body_offset, &(column->list_type));
						if(column->type == CQL_COLUMN_TYPE_MAP)
							body_offset += unpack_short(body_offset, &(column->value_type));
						break;
					}
				}
			}
			rows_result->metadata = metadata;

			body_offset += unpack_long(body_offset, &(rows_result->rows_count));

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
					long bytes_length = 0;
					body_offset += unpack_bytes(body_offset, &bytes, &bytes_length);

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
								printf("BAD BOOLEAN SIZE %d\n", bytes_length); // Handle error
							column_value->length = bytes_length;
							column_value->value = malloc(column_value->length);
							memcpy(column_value->value, bytes, column_value->length);
							break;
						case CQL_COLUMN_TYPE_INT:
							if(bytes_length != 4)
								printf("BAD 32 INT SIZE %d\n", bytes_length); // Handle error
							column_value->length = bytes_length;
							column_value->value = malloc(column_value->length);
							unpack_long(bytes, (long *) &column_value->value);
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

			res->data = rows_result;
		}
		break;
	case CQL_RESULT_KIND_SET_KEYSPACE:
		unpack_string(body_offset, (char **) &(res->data));
		break;
	case CQL_RESULT_KIND_PREPARED:
		// TODO Implement this
		break;
	case CQL_RESULT_KIND_SCHEMA_CHANGE:
		{
			cql_schema_change *sc = malloc(sizeof(cql_schema_change));
			body_offset += unpack_string(body_offset, &(sc->change));
			body_offset += unpack_string(body_offset, &(sc->keyspace));
			body_offset += unpack_string(body_offset, &(sc->table));
			res->data = sc;
		}
		break;
	default:
		cql_result_destroy(res);
		return cql_client_error_create(result, "Unknown result kind %d", res->kind);
	}

	*result = (void*) res;

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
				cql_rows_metadata *metadata = rows_result->metadata;
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

long cql_server_error_create(cql_frame *frame, cql_server_error **error) {
	cql_server_error *err = malloc(sizeof(cql_server_error));
	char *body_offset = frame->body;
	body_offset += unpack_long(body_offset, &(err->code));
	body_offset += unpack_string(body_offset, &err->message);

	switch(err->code) {
	case CQL_ERROR_UNAVAILABLE:
		{
			cql_unavailable *unavailable = malloc(sizeof(cql_unavailable));
			body_offset += unpack_short(body_offset, &(unavailable->consistency));
			body_offset += unpack_long(body_offset, &(unavailable->required_nodes));
			body_offset += unpack_long(body_offset, &(unavailable->alive_nodes));
			err->additional = unavailable;
		}
		break;
	case CQL_ERROR_WRITE_TIMEOUT:
		{
			cql_write_timeout *write_timeout = malloc(sizeof(cql_write_timeout)); // Memory leak
			body_offset += unpack_short(body_offset, &(write_timeout->consistency));
			body_offset += unpack_long(body_offset, &(write_timeout->nodes_received));
			body_offset += unpack_long(body_offset, &(write_timeout->nodes_required));
			body_offset += unpack_string(body_offset, &write_timeout->write_type); // Memory leak

			err->additional = write_timeout;
		}
		break;
	case CQL_ERROR_READ_TIMEOUT:
		{
			cql_read_timeout *read_timeout = malloc(sizeof(cql_read_timeout)); // Memory leak
			body_offset += unpack_short(body_offset, &(read_timeout->consistency));
			body_offset += unpack_long(body_offset, &(read_timeout->nodes_received));
			body_offset += unpack_long(body_offset, &(read_timeout->nodes_required));

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

int round_trip_request_response(cql_connection *connection, cql_frame *request, cql_frame **response) {
	int stream_id;
	if(!(stream_id = send_frame(connection, request)))
		return 0;

	return read_frame(connection, response, stream_id);
}

int send_frame(cql_connection *connection, cql_frame *request) {
	int out_buffer_size = 8 + request->length;
	char out_buffer[out_buffer_size];
	out_buffer[0] = 0x01;
	out_buffer[1] = 0x01;
	out_buffer[2] = connection->next_stream_id++;
	out_buffer[3] = request->opcode;

	pack_long(out_buffer + 4, request->length);
	memcpy(out_buffer + 8, request->body, request->length);

	if(write(connection->fd, out_buffer, out_buffer_size) != out_buffer_size)
		return 0;

	return out_buffer[2];
}

// TODO This function needs to be replaced by epoll/kqueue or something
int read_frame(cql_connection *connection, cql_frame **response, char target_stream_id) {
	const int BLOCK_BUFFER_SIZE = 1024;

	char block_buffer[BLOCK_BUFFER_SIZE];
	char *buffer = NULL;
	int cont = 1;
	int bytes_read = 0;
	int readres;
	while(cont) {
		readres = read(connection->fd, block_buffer, BLOCK_BUFFER_SIZE);
		if(readres > 0) {
			char *new_buffer = malloc(bytes_read + readres);
			if(bytes_read > 0)
				memcpy(&new_buffer, buffer, bytes_read);
			memcpy(new_buffer + bytes_read, block_buffer, readres);
			free(buffer);
			buffer = new_buffer;
			bytes_read += readres;
		}

		cont = readres == 0 || readres == BLOCK_BUFFER_SIZE;
	}

	if(readres == -1)
		return 0;

	unsigned long bytes_processed = 0;
	*response = NULL;
	while(bytes_processed < bytes_read && *response == NULL) {
		if(bytes_read - bytes_processed < 8)
			return 0;

		char version = buffer[0];
		char flags = buffer[1];
		char stream = buffer[2];
		char opcode = buffer[3];
		char *buffer_offset = buffer + 4;

		unsigned long length;
		buffer_offset += unpack_long(buffer_offset, &length);

		char *body = NULL;
		if(length > 0) {
			body = malloc(length);
			memcpy(body, buffer_offset, length);
		}

		// TODO This is lame because we could read and ignore frames
		if(stream == target_stream_id) {
			*response = malloc(sizeof(cql_frame)); // TODO Memory leak
			(*response)->opcode = opcode;
			(*response)->length = length;
			(*response)->body = body;
		}
	}
	free(buffer);

	return *response != NULL;
}

int pack_string_map(cql_string_map *map, unsigned short element_count, char** buffer) {
	int buffer_size = 2, i;
	for(i = 0; i < element_count; i++) {
		buffer_size += strlen(map[i].key) + strlen(map[i].value) + 4;
	}

	*buffer = malloc(buffer_size);
	memset(*buffer, 0, buffer_size);
	pack_short(*buffer, element_count);

	char *buffer_offset = *buffer + 2;
	for(i = 0; i < element_count; i++) {
		char *serialized_key;
		int key_size = pack_string(map[i].key, &serialized_key);
		memcpy(buffer_offset, serialized_key, key_size);
		buffer_offset += key_size;
		free(serialized_key);

		char *serialized_value;
		int value_size = pack_string(map[i].value, &serialized_value);
		memcpy(buffer_offset, serialized_value, value_size);
		buffer_offset += value_size;
		free(serialized_value);
	}

	return buffer_size;
}

int pack_string_common(char *in, char **out, char long_string) {
	unsigned long len = strlen(in);
	char int_size = long_string ? 4 : 2;
	*out = malloc(len + int_size);
	if(long_string)
		pack_long(*out, len);
	else
		pack_short(*out, (unsigned short) len);
	if(len > 0)
		memcpy(*out + int_size, in, len);

	return len + int_size;
}

int unpack_string_common(char *in, char **out, char long_string) {
	char *in_offset = in;
	unsigned long len = 0;
	in_offset += long_string ? unpack_long(in, &len) : unpack_short(in, &len);
	char int_size = long_string ? 4 : 2;
	*out = malloc(len + 1);
	if(len > 0)
		memcpy(*out, in + int_size, len);
	memset(*out + len, 0, 1);

	return int_size + len;
}

int unpack_bytes(char *in, char **out, long *out_length) {
	char *in_offset = in;
	in_offset += unpack_long(in_offset, out_length);
	if(*out_length < 0) {
		*out = NULL;
	} else {
		*out = malloc(*out_length);
		memcpy(*out, in_offset, *out_length);
		in_offset += *out_length;
	}

	return in_offset - in;
}

int unpack_string_multimap(char *in, int in_len, cql_string_multimap **out) {
	char *in_offset = in;
	unsigned short size = 0;
	in_offset += unpack_short(in_offset, &size);

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
	in_offset += unpack_short(in_offset, &((*list)->values_count));

	(*list)->values = malloc(sizeof(char*) * (*list)->values_count);
	memset((*list)->values, 0, (*list)->values_count);

	int i;
	for(i = 0; i < (*list)->values_count; i++) {
		in_offset += unpack_string(in_offset, (*list)->values[i]);
	}

	return in_offset - in;
}

uint32_t pack_long(char *buffer, uint32_t i) {
	uint32_t n = htonl(i);
	memcpy(buffer, &n, sizeof(n));
	return n;
}

uint16_t pack_short(char *buffer, uint16_t i) {
	uint16_t n = htons(i);
	memcpy(buffer, &n, sizeof(n));
	return n;
}

int unpack_long(char *buffer, long *n) {
	memcpy(n, buffer, 4);
	*n = ntohl(*n);
	return 4;
}

int unpack_short(char *buffer, short *n) {
	memcpy(n, buffer, 2);
	*n = ntohs(*n);
	return 2;
}


