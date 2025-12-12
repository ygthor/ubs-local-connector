<?php
if (!isset($_SESSION)) session_start();

class mysql
{
	protected $con;
	protected $in_transaction = false; // Track transaction state
	
	public function __construct()
	{
		$this->connect();
	}

	function connect()
	{
		$this->con = mysqli_connect(ENV::DB_HOST, ENV::DB_USERNAME, ENV::DB_PASSWORD);
		mysqli_select_db($this->con, ENV::DB_NAME);
	}

	function connect_remote()
	{
		$this->con = mysqli_connect(ENV::REMOTE_DB_HOST, ENV::REMOTE_DB_USERNAME, ENV::REMOTE_DB_PASSWORD);
		mysqli_select_db($this->con, ENV::REMOTE_DB_NAME);
	}

	function query($query)
	{
		if (!$query) return null;
		$rs = mysqli_query($this->con, $query);
		return $rs;
	}

	function insert_id()
	{
		$query = mysqli_insert_id($this->con);
		return $query;
	}

	function escape($string)
	{
		$string = mysqli_real_escape_string($this->con, $string);
		return $string;
	}

	function close()
	{
		mysqli_close($this->con);
	}

	function insert($table_name, $arr)
	{
		$columns_arr = [];
		$values_arr = [];

		foreach ($arr as $k => $v) {
			if (
				is_numeric($v)
				&& substr($v, 0, 1) != '0' // PREVENT Postcode & IC issue
			) {
				$value = $v;
			} elseif (is_array($v)) {
				$value = count($v) == 0 ? 'null' : implode(',', $v);
				if ($value != 'null') $value = "'$value'";
			} elseif ($v === null) {
				$value = 'null';
			} else {
				$value = '"' . $this->escape($v) . '"';
			}

			$columns_arr[] = "`$k`";
			$values_arr[] = $value;
		}

		$columns = implode(',', $columns_arr);
		$values = implode(',', $values_arr);

		if (strpos($table_name, '.') !== false) {
			$sql = "INSERT INTO " . $table_name . " ($columns) VALUES ($values)";
		} else {
			$sql = "INSERT INTO `" . $table_name . "` ($columns) VALUES ($values)";
		}

		$this->SQL_EXECUTED[] = $sql;
		mysqli_query($this->con, $sql);

		//check primary

		$id = mysqli_insert_id($this->con);
		if (mysqli_error($this->con)) {
			dump2("MYSQL ERROR INSERT: " . mysqli_error($this->con));
			dump2($sql);
			debug();
		}


		return $id;
	}

	function update($table_name, $condition, $arr)
	{
		if (empty($condition)) {
			echo ('ERROR: update_or_insert\' $condition cannot be empty');
			return;
		}
		if (empty($arr)) {
			echo ('ERROR: update_or_insert\' $arr cannot be empty');
			return;
		}

		$update_arr = [];

		foreach ($arr as $k => $v) {
			if (
				is_numeric($v)
				&& substr($v, 0, 1) != '0' // PREVENT Postcode & IC issue
			) {
				$value = $v;
			} elseif (is_callable($v)) {
				// becareful using this due to it bypass the db escape below.
				// useful when we want to update the data with mysql function or based on another column data
				$value = $v();
			} elseif (is_array($v)) {
				$value = count($v) == 0 ? 'null' : "'" . implode(',', $v) . "'";
			} elseif ($v == null) {
				$value = 'null';
			} else {
				$value = '"' . $this->escape($v) . '"';
			}
			$update_arr[] = " `$k` = $value ";
		}

		$update = implode(',', $update_arr);

		if (strpos($table_name, '.')) {
			$table_arr = explode('.', $table_name);
			$str_table_name =   $table_arr[0] . '.' . $table_arr[1];
		} else {
			$str_table_name = "`$table_name`";
		}

		$sql_condition = '';
		foreach ($condition as $key => $val) {
			if (is_array($val)) {
				$sql_condition .= " AND `$key` IN (" . implode(',', $val) . ")";
			} else if ($val === null) {
				$sql_condition .= " AND `$key` IS NULL";
			} else {
				$sql_condition .= " AND `$key` = '" . $this->escape($val) . "'";
			}
		}

		// ✅ FIX: For orders table with reference_no, ensure we only update ONE record
		// If multiple records match, delete duplicates first, then update the remaining one
		if (strpos($str_table_name, 'orders') !== false && isset($condition['reference_no'])) {
			// First check if there are multiple records
			$check_duplicates_sql = "SELECT id, updated_at FROM $str_table_name WHERE 1 $sql_condition ORDER BY updated_at DESC, id DESC";
			$duplicates = $this->get($check_duplicates_sql);
			
			if (count($duplicates) > 1) {
				// Multiple records found - delete old ones, keep most recent
				$keep_id = $duplicates[0]['id'];
				$delete_ids = array_column(array_slice($duplicates, 1), 'id');
				
				if (!empty($delete_ids)) {
					$delete_ids_str = implode(',', array_map('intval', $delete_ids));
					$delete_sql = "DELETE FROM $str_table_name WHERE id IN ($delete_ids_str)";
					$this->query($delete_sql);
					dump("⚠️  Deleted " . count($delete_ids) . " duplicate order(s) before update, kept ID: $keep_id");
				}
				
				// Now update only the remaining record by ID (more specific condition)
				$sql_condition = " AND id = $keep_id";
			}
			// If only one record exists, proceed with normal update
		}

		$sql = "
			UPDATE $str_table_name SET
			$update
			WHERE 1 $sql_condition
			";
		$this->SQL_EXECUTED[] = $sql;
		$data = mysqli_query($this->con, $sql);
		if (mysqli_error($this->con)) {
			dump($sql);
			dump2("MYSQL ERROR: UPDATE" . mysqli_error($this->con));
		}
		return $data;
	}

	function delete($table_name, $condition)
	{
		if (empty($condition)) {
			echo ('ERROR: DELETE \' $condition cannot be empty');
			return;
		}
		if (strpos($table_name, '.')) {
			$table_arr = explode('.', $table_name);
			$str_table_name =   $table_arr[0] . '.' . $table_arr[1];
		} else {
			$str_table_name = "`$table_name`";
		}

		$sql_condition = '';
		foreach ($condition as $key => $val) {
			if (is_array($val)) {
				$sql_condition .= " AND $key IN (" . implode(',', $val) . ")";
			} else {
				$sql_condition .= " AND $key = '$val'";
			}
		}
		if (empty($sql_condition)) {
			echo ('ERROR: DELETE \' $condition cannot be empty');
			return;
		}

		$sql = "
			DELETE FROM $str_table_name
			WHERE 1 $sql_condition
			";

		$data = mysqli_query($this->con, $sql);
		if (mysqli_error($this->con)) {
			dump2("MYSQL ERROR DELETE: " . mysqli_error($this->con));
		}
		return $data;
	}

	function get($rs)
	{
		if (is_string($rs)) {
			$rs = $this->query($rs);
		}

		if (is_bool($rs) && isDev()) {
			debug();
		}

		$data = [];
		while ($row = mysqli_fetch_assoc($rs)) {
			$data[] = $row;
		}
		return $data;
	}

	function first($rs)
	{
		if (is_string($rs)) {
			$sql = $rs;
			$rs = $this->query($sql);
			if ($rs == false) {
				dump2("ERR: " . $sql);
				dump2("MYSQL ERROR FIRST: " . mysqli_error($this->con));
			}
		}

		//TODO VALIDATE IS RS
		$data = [];
		while ($row = mysqli_fetch_assoc($rs)) {
			$data[] = $row;
		}
		return count($data) > 0 ? current($data) : null;
	}

	function pluck($rs, $col_value, $col_key = '')
	{
		if (is_string($rs)) {
			$rs = $this->query($rs);
		}

	$data = [];
	while ($row = mysqli_fetch_assoc($rs)) {
		$row_col_value = isset($row[$col_value]) ? $row[$col_value] : null;
		if (!empty($col_key)) {
			// Check if the key column exists in the row
			if (isset($row[$col_key])) {
				$data[$row[$col_key]] = $row_col_value;
			} else {
				// Key column doesn't exist, skip this row or use a fallback
				// Log warning for debugging
				dump("⚠️  Warning: Column '$col_key' not found in pluck result. Available columns: " . implode(', ', array_keys($row)));
			}
		} else {
			$data[] = $row_col_value;
		}
	}
	return $data;
	}

	function update_or_insert($table, $condition, $arr)
	{
		if (empty($condition)) {
			echo ('ERROR: update_or_insert\' $condition cannot be empty');
			return;
		}
		if (empty($arr)) {
			echo ('ERROR: update_or_insert\' $arr cannot be empty');
			return;
		}

		$table_name = strpos($table, '.') !== false ? $table : "`$table`";
		
		$sql_check = "SELECT * FROM $table_name WHERE 1 ";
		foreach ($condition as $key => $val) {
			$sql_check .= " AND `$key` = '" . $this->escape($val) . "'";
		}
		
		// ✅ FIX: Get ALL matching records, not just first (to handle duplicates)
		$rows_check = $this->get($sql_check);

		if (empty($rows_check)) {
			// No record found, insert new
			return $this->insert($table, $arr);
		} else {
			// ✅ FIX: If multiple records found (duplicates), delete old ones and keep/update the most recent
			if (count($rows_check) > 1) {
				// Find the most recent record (by updated_at or id)
				$most_recent = null;
				$most_recent_time = 0;
				$most_recent_id = 0;
				
				foreach ($rows_check as $row) {
					$updated_at = $row['updated_at'] ?? $row['UPDATED_ON'] ?? null;
					$row_time = $updated_at ? strtotime($updated_at) : 0;
					$row_id = $row['id'] ?? 0;
					
					if ($row_time > $most_recent_time || ($row_time == $most_recent_time && $row_id > $most_recent_id)) {
						$most_recent_time = $row_time;
						$most_recent_id = $row_id;
						$most_recent = $row;
					}
				}
				
				// Delete all duplicates except the most recent one
				$delete_ids = [];
				foreach ($rows_check as $row) {
					$row_id = $row['id'] ?? null;
					if ($row_id && $row_id != $most_recent_id) {
						$delete_ids[] = (int)$row_id;
					}
				}
				
				if (!empty($delete_ids)) {
					$delete_ids_str = implode(',', $delete_ids);
					$delete_sql = "DELETE FROM $table_name WHERE id IN ($delete_ids_str)";
					$this->query($delete_sql);
					dump("⚠️  Deleted " . count($delete_ids) . " duplicate record(s) in $table, kept ID: $most_recent_id");
				}
				
				// Use the most recent record for update
				$row_check = $most_recent;
			} else {
				$row_check = $rows_check[0];
			}
			
			// Update the existing record
			$result = $this->update($table, $condition, $arr);
			if ($result === true) {
				$sql_primary_key = "SHOW KEYS FROM $table_name WHERE Key_name = 'PRIMARY'";
				$row_primary_key = $this->first($sql_primary_key);
				$primary_key = $row_primary_key['Column_name'] ?? 'id';

				return $row_check[$primary_key] ?? $row_check['id'] ?? null;
			} else {
				return $result;
			}
		}
	}

	// High-performance bulk upsert method
	function bulkUpsert($table, $records, $primaryKey)
	{
		if (empty($records)) {
			return;
		}

		$table_name = strpos($table, '.') !== false ? $table : "`$table`";
		
		// ✅ FIX: Check for existing records first (since ON DUPLICATE KEY UPDATE requires UNIQUE constraint)
		// Get all existing keys to prevent duplicates
		$existing_keys = [];
		if (!empty($primaryKey)) {
			$keys_to_check = array_filter(array_column($records, $primaryKey));
			if (!empty($keys_to_check)) {
				$keys_escaped = array_map(function($key) {
					return "'" . $this->escape($key) . "'";
				}, array_unique($keys_to_check));
				
				$check_sql = "SELECT `$primaryKey`, id, updated_at FROM $table_name WHERE `$primaryKey` IN (" . implode(',', $keys_escaped) . ")";
				$existing_records = $this->get($check_sql);
				
				// Group by primary key to find duplicates
				foreach ($existing_records as $existing) {
					$key = $existing[$primaryKey] ?? null;
					if ($key !== null) {
						if (!isset($existing_keys[$key])) {
							$existing_keys[$key] = [];
						}
						$existing_keys[$key][] = $existing;
					}
				}
				
				// Delete duplicate records (keep most recent)
				foreach ($existing_keys as $key => $duplicates) {
					if (count($duplicates) > 1) {
						// Sort by updated_at descending, keep first
						usort($duplicates, function($a, $b) {
							$time_a = isset($a['updated_at']) ? strtotime($a['updated_at']) : 0;
							$time_b = isset($b['updated_at']) ? strtotime($b['updated_at']) : 0;
							return $time_b <=> $time_a;
						});
						
						$keep_id = $duplicates[0]['id'];
						$delete_ids = array_column(array_slice($duplicates, 1), 'id');
						
						if (!empty($delete_ids)) {
							$delete_ids_str = implode(',', array_map('intval', $delete_ids));
							$delete_sql = "DELETE FROM $table_name WHERE id IN ($delete_ids_str)";
							$this->query($delete_sql);
							dump("⚠️  Deleted " . count($delete_ids) . " duplicate record(s) with $primaryKey='$key', kept ID: $keep_id");
						}
						
						// Keep only the most recent one
						$existing_keys[$key] = [$duplicates[0]];
					}
				}
			}
		}
		
		// Get all columns from first record
		$columns = array_keys($records[0]);
		$columns_str = '`' . implode('`, `', $columns) . '`';
		
		// Build VALUES clause - filter out records that already exist
		$values = [];
		$records_to_insert = [];
		foreach ($records as $record) {
			$key_value = $record[$primaryKey] ?? null;
			
			// Skip if record already exists (we'll update it separately)
			if ($key_value !== null && isset($existing_keys[$key_value])) {
				// ✅ FIX: Record exists - first ensure only ONE record exists (delete duplicates)
				$existing = $existing_keys[$key_value];
				if (count($existing) > 1) {
					// Multiple duplicates found - delete all except the most recent
					usort($existing, function($a, $b) {
						$time_a = isset($a['updated_at']) ? strtotime($a['updated_at']) : 0;
						$time_b = isset($b['updated_at']) ? strtotime($b['updated_at']) : 0;
						return $time_b <=> $time_a;
					});
					
					$keep_id = $existing[0]['id'];
					$delete_ids = array_column(array_slice($existing, 1), 'id');
					
					if (!empty($delete_ids)) {
						$delete_ids_str = implode(',', array_map('intval', $delete_ids));
						$delete_sql = "DELETE FROM $table_name WHERE id IN ($delete_ids_str)";
						$this->query($delete_sql);
						dump("⚠️  Deleted " . count($delete_ids) . " duplicate record(s) with $primaryKey='$key_value', kept ID: $keep_id");
					}
					
					// Update the remaining record
					$this->update($table, [$primaryKey => $key_value], $record);
				} else {
					// Only one record exists, update it
					$this->update($table, [$primaryKey => $key_value], $record);
				}
				continue;
			}
			
			$records_to_insert[] = $record;
			$record_values = [];
			foreach ($columns as $column) {
				$value = $record[$column] ?? null;
				
				if (is_numeric($value) && substr($value, 0, 1) != '0') {
					$record_values[] = $value;
				} elseif (is_array($value)) {
					$val = count($value) == 0 ? 'null' : implode(',', $value);
					$record_values[] = $val != 'null' ? "'$val'" : 'null';
				} elseif ($value === null) {
					$record_values[] = 'null';
				} else {
					$record_values[] = '"' . $this->escape($value) . '"';
				}
			}
			$values[] = '(' . implode(',', $record_values) . ')';
		}
		
		// Only insert if there are new records
		if (empty($values)) {
			return true; // All records already existed and were updated
		}
		
		// ✅ FIX: Final safety check - verify records don't exist right before insert
		// This catches any records that might have been inserted between our check and now
		if (!empty($primaryKey) && !empty($records_to_insert)) {
			$final_check_keys = [];
			$record_key_map = []; // Map key_value to record index
			
			foreach ($records_to_insert as $idx => $record) {
				$key_value = $record[$primaryKey] ?? null;
				if ($key_value !== null) {
					$final_check_keys[] = "'" . $this->escape($key_value) . "'";
					if (!isset($record_key_map[$key_value])) {
						$record_key_map[$key_value] = [];
					}
					$record_key_map[$key_value][] = $idx;
				}
			}
			
			if (!empty($final_check_keys)) {
				$final_check_sql = "SELECT `$primaryKey`, id FROM $table_name WHERE `$primaryKey` IN (" . implode(',', array_unique($final_check_keys)) . ")";
				$final_existing = $this->get($final_check_sql);
				
				// Remove records that now exist from the insert list
				if (!empty($final_existing)) {
					$existing_key_values = array_column($final_existing, $primaryKey);
					$indices_to_remove = [];
					
					foreach ($existing_key_values as $existing_key) {
						if (isset($record_key_map[$existing_key])) {
							foreach ($record_key_map[$existing_key] as $idx) {
								$indices_to_remove[$idx] = true;
								// Record now exists, update it instead
								$this->update($table, [$primaryKey => $existing_key], $records_to_insert[$idx]);
								dump("⚠️  Record with $primaryKey='$existing_key' was inserted between check and insert - updated instead");
							}
						}
					}
					
					// Rebuild arrays without the records that now exist
					$new_records_to_insert = [];
					$new_values = [];
					
					foreach ($records_to_insert as $idx => $record) {
						if (!isset($indices_to_remove[$idx])) {
							$new_records_to_insert[] = $record;
							$new_values[] = $values[$idx];
						}
					}
					
					$records_to_insert = $new_records_to_insert;
					$values = $new_values;
					
					if (empty($values)) {
						return true; // All records were updated instead
					}
				}
			}
		}
		
		// Execute bulk insert (no ON DUPLICATE KEY UPDATE since we checked already)
		$sql = "INSERT INTO $table_name ($columns_str) VALUES " . implode(',', $values);
		
		$this->SQL_EXECUTED[] = $sql;
		$result = mysqli_query($this->con, $sql);
		
		if (mysqli_error($this->con)) {
			$error = mysqli_error($this->con);
			dump("MYSQL ERROR BULK UPSERT: " . $error);
			dump($sql);
			throw new Exception("MySQL bulk upsert error: $error");
		}
		
		if (!$result) {
			$error = mysqli_error($this->con) ?: "Unknown error";
			throw new Exception("MySQL bulk upsert failed: $error");
		}
		
		$affectedRows = mysqli_affected_rows($this->con);
		if ($affectedRows === 0 && count($records_to_insert) > 0) {
			// Warning: No rows affected but we tried to insert
			dump("WARNING: bulkUpsert executed but 0 rows affected for table $table_name");
		}
		
		return $result;
	}

	// ✅ SAFE: Transaction support for data integrity
	function beginTransaction()
	{
		$this->in_transaction = true;
		return mysqli_begin_transaction($this->con);
	}

	function commit()
	{
		$this->in_transaction = false;
		return mysqli_commit($this->con);
	}

	function rollback()
	{
		$this->in_transaction = false;
		return mysqli_rollback($this->con);
	}

	function inTransaction()
	{
		return $this->in_transaction === true;
	}
}
