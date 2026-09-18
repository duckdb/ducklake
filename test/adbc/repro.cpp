#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/common/adbc/adbc.hpp"

extern "C" AdbcStatusCode duckdb_adbc_init(int version, void *driver, AdbcError *error);

namespace {

AdbcDriver driver {};
std::atomic<bool> catalog_failure_seen {false};
std::atomic<bool> invalidation_seen {false};
std::mutex output_mutex;

void ReleaseError(AdbcError &error) {
	if (error.release) {
		error.release(&error);
	}
	error = {};
}

std::string ErrorMessage(const AdbcError &error) {
	return error.message ? error.message : "";
}

void ObserveError(std::string_view operation, std::string_view sql, std::string_view message) {
	if (message.find("database has been invalidated") != std::string_view::npos) {
		invalidation_seen = true;
		catalog_failure_seen = true;
	} else if (message.find("CatalogEntry") != std::string_view::npos ||
	           message.find("INTERNAL Error") != std::string_view::npos) {
		catalog_failure_seen = true;
	} else {
		// These two errors are deliberate parts of the workload.
		if (message.find("Binder Error") != std::string_view::npos ||
		    (sql == "ROLLBACK" && message.find("no transaction is active") != std::string_view::npos)) {
			return;
		}
	}

	std::lock_guard<std::mutex> lock(output_mutex);
	std::cerr << operation << "\nSQL: " << sql << "\nERROR: " << message << "\n";
}

bool RequireSuccess(AdbcStatusCode status, AdbcError &error, std::string_view operation) {
	if (status == ADBC_STATUS_OK) {
		ReleaseError(error);
		return true;
	}

	std::cerr << "Setup failed during " << operation << ": " << ErrorMessage(error) << "\n";
	ReleaseError(error);
	return false;
}

AdbcStatusCode Execute(AdbcConnection &connection, const std::string &sql) {
	AdbcError error {};
	AdbcStatement statement {};
	ArrowArrayStream stream {};
	int64_t rows_affected = 0;

	auto status = driver.StatementNew(&connection, &statement, &error);
	if (status != ADBC_STATUS_OK) {
		ObserveError("StatementNew failed", sql, ErrorMessage(error));
		ReleaseError(error);
		return status;
	}

	status = driver.StatementSetSqlQuery(&statement, sql.c_str(), &error);
	if (status == ADBC_STATUS_OK) {
		status = driver.StatementExecuteQuery(&statement, &stream, &rows_affected, &error);
	}
	if (status != ADBC_STATUS_OK) {
		ObserveError("Statement execution failed", sql, ErrorMessage(error));
	}
	ReleaseError(error);

	if (stream.release) {
		ArrowSchema schema {};
		int stream_status = stream.get_schema(&stream, &schema);
		if (stream_status != 0) {
			const auto *message = stream.get_last_error(&stream);
			ObserveError("Arrow schema fetch failed", sql, message ? message : "unknown error");
			status = ADBC_STATUS_INTERNAL;
		}
		if (schema.release) {
			schema.release(&schema);
		}

		while (stream_status == 0) {
			ArrowArray batch {};
			stream_status = stream.get_next(&stream, &batch);
			if (stream_status != 0) {
				const auto *message = stream.get_last_error(&stream);
				ObserveError("Arrow batch fetch failed", sql, message ? message : "unknown error");
				status = ADBC_STATUS_INTERNAL;
				if (batch.release) {
					batch.release(&batch);
				}
				break;
			}
			if (!batch.release) {
				break;
			}
			batch.release(&batch);
		}
		stream.release(&stream);
	}

	AdbcError release_error {};
	auto release_status = driver.StatementRelease(&statement, &release_error);
	if (release_status != ADBC_STATUS_OK) {
		ObserveError("StatementRelease failed", sql, ErrorMessage(release_error));
	}
	ReleaseError(release_error);
	return status;
}

bool SetAutocommit(AdbcConnection &connection) {
	AdbcError error {};
	auto status = driver.ConnectionSetOption(&connection, "adbc.connection.autocommit", "true", &error);
	if (status != ADBC_STATUS_OK) {
		ObserveError("Setting autocommit failed", "adbc.connection.autocommit=true", ErrorMessage(error));
	}
	ReleaseError(error);
	return status == ADBC_STATUS_OK;
}

} // namespace

int main(int argc, char **argv) {
	std::cout << std::unitbuf;
	if (argc < 6 || argc > 7) {
		std::cerr << "Usage: " << argv[0]
		          << " <database> <port> <data-path> <ducklake-extension> <postgres-extension> [duration-seconds]\n";
		return 2;
	}

	const std::string database_name = argv[1];
	const std::string postgres_port = argv[2];
	const std::filesystem::path data_path = std::filesystem::absolute(argv[3]);
	const std::string ducklake_extension = argv[4];
	const std::string postgres_extension = argv[5];
	const int duration_seconds = argc == 7 ? std::stoi(argv[6]) : 15;
	std::filesystem::create_directories(data_path);

	AdbcError error {};
	if (!RequireSuccess(duckdb_adbc_init(ADBC_VERSION_1_1_0, &driver, &error), error, "driver initialization")) {
		return 2;
	}

	AdbcDatabase database {};
	if (!RequireSuccess(driver.DatabaseNew(&database, &error), error, "DatabaseNew") ||
	    !RequireSuccess(driver.DatabaseInit(&database, &error), error, "DatabaseInit")) {
		return 2;
	}

	AdbcConnection setup {};
	if (!RequireSuccess(driver.ConnectionNew(&setup, &error), error, "setup ConnectionNew") ||
	    !RequireSuccess(driver.ConnectionInit(&setup, &database, &error), error, "setup ConnectionInit")) {
		return 2;
	}

	const std::vector<std::string> setup_sql = {
	    "LOAD '" + ducklake_extension + "'",
	    "LOAD '" + postgres_extension + "'",
	    "SET pg_null_byte_replacement=''",
	    "ATTACH 'ducklake:postgres:host=127.0.0.1 port=" + postgres_port + " dbname=" + database_name +
	        " user=postgres password=postgres' AS lake (DATA_PATH '" + data_path.string() + "', ENCRYPTED)",
	    "SET threads=1",
	    "SET preserve_insertion_order=false",
	    "CREATE SCHEMA lake.app_data",
	};

	for (const auto &sql : setup_sql) {
		if (Execute(setup, sql) != ADBC_STATUS_OK) {
			return 2;
		}
	}
	for (int table = 0; table < 20; ++table) {
		const auto sql = "CREATE TABLE lake.app_data.table_" + std::to_string(table) +
		                 " (item_id VARCHAR, lookup_id VARCHAR, revision BIGINT, status VARCHAR)";
		if (Execute(setup, sql) != ADBC_STATUS_OK) {
			return 2;
		}
	}
	if (Execute(setup, "INSERT INTO lake.app_data.table_0 VALUES ('item-1','lookup-1',1,'active')") != ADBC_STATUS_OK) {
		return 2;
	}

	std::atomic<int> ready {0};
	std::atomic<bool> start {false};
	std::atomic<bool> worker_setup_failed {false};
	std::vector<std::thread> workers;
	std::cout << "Starting eight concurrent ADBC workers for " << duration_seconds << " seconds\n";

	for (int worker = 0; worker < 8; ++worker) {
		workers.emplace_back([&, worker] {
			AdbcError worker_error {};
			AdbcConnection connection {};
			if (!RequireSuccess(driver.ConnectionNew(&connection, &worker_error), worker_error,
			                    "worker ConnectionNew") ||
			    !RequireSuccess(driver.ConnectionInit(&connection, &database, &worker_error), worker_error,
			                    "worker ConnectionInit")) {
				catalog_failure_seen = true;
				worker_setup_failed = true;
				return;
			}

			++ready;
			while (!start) {
				std::this_thread::yield();
			}

			const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(duration_seconds);
			int iteration = 0;
			while (std::chrono::steady_clock::now() < deadline && !catalog_failure_seen) {
				Execute(connection, "SET TimeZone='UTC'");
				Execute(connection, "SET pg_null_byte_replacement=''");
				SetAutocommit(connection);
				Execute(connection, "SET schema='lake.app_data'");

				std::string sql;
				switch (worker) {
				case 0:
					sql = "UPDATE table_0 SET revision=" + std::to_string(iteration) + " WHERE item_id='item-1'";
					break;
				case 1:
					sql = "CREATE OR REPLACE TABLE lake.app_data.replaced_table AS SELECT 1 AS value";
					break;
				case 2:
					sql = "WITH filtered_items AS (SELECT * FROM lake.app_data.table_0) "
					      "SELECT * FROM filtered_items WHERE \"lookup id\" = 'lookup-1' LIMIT 5";
					break;
				case 3:
					sql = "SELECT * FROM table_0";
					break;
				case 4:
					sql = "SELECT * FROM duckdb_columns() WHERE database_name='lake'";
					break;
				default:
					sql = "SELECT table_name AS name, COALESCE(comment,'') AS comment FROM duckdb_tables() "
					      "WHERE database_name='lake' AND schema_name='app_data' ORDER BY table_name";
					break;
				}

				Execute(connection, sql);
				Execute(connection, "SET schema='main'");
				Execute(connection, "ROLLBACK");
				SetAutocommit(connection);
				++iteration;
			}

			AdbcError release_error {};
			driver.ConnectionRelease(&connection, &release_error);
			ReleaseError(release_error);
		});
	}

	while (ready < 8 && !catalog_failure_seen) {
		std::this_thread::yield();
	}
	start = true;
	for (auto &worker : workers) {
		worker.join();
	}

	std::cout << "Running final health query\n";
	const auto health_status = Execute(setup, "SELECT * FROM lake.app_data.table_0");

	AdbcError release_error {};
	driver.ConnectionRelease(&setup, &release_error);
	ReleaseError(release_error);
	driver.DatabaseRelease(&database, &release_error);
	ReleaseError(release_error);

	if (invalidation_seen) {
		std::cout << "REPRODUCED: the shared DuckDB database was invalidated\n";
		return 1;
	}
	if (worker_setup_failed || health_status != ADBC_STATUS_OK) {
		std::cerr << "INCONCLUSIVE: worker setup or final health query failed\n";
		return -1;
	}

	std::cerr << "NOT REPRODUCED: no database-invalidated error was observed in this attempt\n";
	return 0;
}
