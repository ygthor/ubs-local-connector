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
				$sql_condition .= " AND $key IN (" . implode(',', $val) . ")";
			} else if ($val === null) {
				$sql_condition .= " AND $key IS NULL";
			} else {
				$sql_condition .= " AND $key = '$val'";
			}
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
				$data[$row[$col_key]] = $row_col_value;
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

		$sql_check = "SELECT * FROM $table WHERE 1 ";
		foreach ($condition as $key => $val) {
			$sql_check .= " AND $key = '$val'";
		}
		$row_check = $this->first($sql_check);

		if ($row_check == null) {
			return $this->insert($table, $arr);
		} else {
			$result =  $this->update($table, $condition, $arr);
			if ($result === true) {
				$sql_primary_key = "SHOW KEYS FROM $table WHERE Key_name = 'PRIMARY'";
				$row_primary_key = $this->first($sql_primary_key);
				$primary_key = $row_primary_key['Column_name'];

				return $row_check[$primary_key];
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
		
		// Get all columns from first record
		$columns = array_keys($records[0]);
		$columns_str = '`' . implode('`, `', $columns) . '`';
		
		// Build VALUES clause
		$values = [];
		foreach ($records as $record) {
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
		
		// Build ON DUPLICATE KEY UPDATE clause
		$update_clause = [];
		foreach ($columns as $column) {
			if ($column !== $primaryKey) {
				$update_clause[] = "`$column` = VALUES(`$column`)";
			}
		}
		$update_str = implode(', ', $update_clause);
		
		// Execute bulk upsert
		$sql = "INSERT INTO $table_name ($columns_str) VALUES " . implode(',', $values) . 
			   " ON DUPLICATE KEY UPDATE $update_str";
		
		$this->SQL_EXECUTED[] = $sql;
		$result = mysqli_query($this->con, $sql);
		
		if (mysqli_error($this->con)) {
			dump("MYSQL ERROR BULK UPSERT: " . mysqli_error($this->con));
			dump($sql);
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
