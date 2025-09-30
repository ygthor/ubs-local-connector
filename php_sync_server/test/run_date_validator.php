<?php
/**
 * Simple runner script for DBF Date Validator
 * 
 * Usage: php run_date_validator.php
 */

echo "🚀 Starting DBF Date Validation...\n";
echo "This script will validate all date fields in UBS DBF files.\n\n";

// Include the validator
require_once __DIR__ . '/validate_dbf_dates.php';
