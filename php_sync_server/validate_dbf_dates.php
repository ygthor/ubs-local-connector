<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

/**
 * DBF Date Validator Script
 * 
 * This script reads DBF files and identifies fields with invalid dates
 * Flow:
 * 1. Define array of DBF tables to loop through
 * 2. Read DBF structure for each table
 * 3. Load data and validate date columns
 * 4. Report invalid dates found
 */

class DBFDateValidator
{
    private $invalidDates = [];
    private $totalRecordsProcessed = 0;
    private $totalInvalidDates = 0;
    
    /**
     * Get list of DBF tables to validate (similar to main.php)
     */
    public function getDBFTables()
    {
        return [
            'ubs_ubsacc2015_arcust',
            'ubs_ubsstk2015_arpso',
            'ubs_ubsstk2015_icpso',
            'ubs_ubsstk2015_artran',
            'ubs_ubsstk2015_ictran',
            'ubs_ubsacc2015_gldata',
        ];
    }
    
    /**
     * Parse UBS table name to get database and table info
     */
    private function parseUbsTable($input)
    {
        $parts = explode('_', $input, 3);
        
        if (count($parts) === 3 && $parts[0] === 'ubs') {
            return [
                'database' => $parts[1],
                'table' => $parts[2]
            ];
        }
        
        return null;
    }
    
    /**
     * Get DBF file path for a table
     */
    private function getDBFPath($tableName)
    {
        $arr = $this->parseUbsTable($tableName);
        if (!$arr) {
            throw new Exception("Invalid table format: $tableName");
        }
        
        $directory = strtoupper($arr['database']);
        $table = $arr['table'];
        
        return "C:/$directory/" . ENV::DBF_SUBPATH . "/{$table}.dbf";
    }
    
    /**
     * Read DBF structure and identify date columns
     */
    private function getDBFStructure($dbfPath)
    {
        try {
            $table = new XBase\TableReader($dbfPath, [
                'encoding' => 'cp1252',
            ]);
            
            $structure = [];
            $dateColumns = [];
            
            foreach ($table->getColumns() as $column) {
                $columnInfo = [
                    'name' => $column->getName(),
                    'type' => $column->getType(),
                    'size' => $column->getLength(),
                    'decimals' => $column->getDecimalCount(),
                ];
                
                $structure[] = $columnInfo;
                
                // Identify date columns (type 'D' for date, 'T' for timestamp)
                if (in_array($column->getType(), ['D', 'T'])) {
                    $dateColumns[] = $column->getName();
                }
            }
            
            $table->close();
            
            return [
                'structure' => $structure,
                'dateColumns' => $dateColumns,
                'totalColumns' => count($structure)
            ];
            
        } catch (Exception $e) {
            throw new Exception("Failed to read DBF structure from $dbfPath: " . $e->getMessage());
        }
    }
    
    /**
     * Validate if a date value is valid
     */
    private function isValidDate($value, $fieldName)
    {
        // Check for empty or null values
        if (empty($value) || $value === null || $value === '') {
            return false;
        }
        
        // Check for common invalid date patterns
        $invalidPatterns = [
            '0000-00-00',
            '0000-00-00 00:00:00',
            '1900-01-01', // Often used as default invalid date
            '1970-01-01', // Unix epoch, often used as default
        ];
        
        if (in_array($value, $invalidPatterns)) {
            return false;
        }
        
        // Try to parse the date
        $timestamp = strtotime($value);
        if ($timestamp === false) {
            return false;
        }
        
        // Check if the parsed date is reasonable (not too far in past/future)
        $currentYear = date('Y');
        $parsedYear = date('Y', $timestamp);
        
        // Allow dates from 1900 to 50 years in the future
        if ($parsedYear < 1900 || $parsedYear > ($currentYear + 50)) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Process DBF file and validate date fields
     */
    private function validateDBFFile($tableName)
    {
        $dbfPath = $this->getDBFPath($tableName);
        
        if (!file_exists($dbfPath)) {
            ProgressDisplay::warning("DBF file not found: $dbfPath");
            return;
        }
        
        ProgressDisplay::info("Processing table: $tableName");
        ProgressDisplay::info("DBF Path: $dbfPath");
        
        try {
            // Get structure and date columns
            $structureInfo = $this->getDBFStructure($dbfPath);
            $dateColumns = $structureInfo['dateColumns'];
            
            ProgressDisplay::info("Found " . count($dateColumns) . " date columns: " . implode(', ', $dateColumns));
            
            if (empty($dateColumns)) {
                ProgressDisplay::info("No date columns found in $tableName, skipping...");
                return;
            }
            
            // Read and validate data
            $table = new XBase\TableReader($dbfPath, [
                'encoding' => 'cp1252',
            ]);
            
            $recordCount = 0;
            $invalidCount = 0;
            $batchSize = 1000;
            
            while ($record = $table->nextRecord()) {
                if ($record->isDeleted()) continue;
                
                $recordCount++;
                
                // Check each date column
                foreach ($dateColumns as $dateColumn) {
                    $value = $record->$dateColumn;
                    
                    // Convert DateTime objects to string for validation
                    if ($value instanceof \DateTimeInterface) {
                        $value = $value->format('Y-m-d');
                    } else {
                        $value = trim((string)$value);
                    }
                    
                    if (!$this->isValidDate($value, $dateColumn)) {
                        $invalidCount++;
                        $this->totalInvalidDates++;
                        
                        $this->invalidDates[] = [
                            'table' => $tableName,
                            'field' => $dateColumn,
                            'value' => $value,
                            'record_number' => $recordCount,
                            'timestamp' => date('Y-m-d H:i:s')
                        ];
                        
                        // Limit the number of invalid dates stored in memory
                        if (count($this->invalidDates) > 10000) {
                            ProgressDisplay::warning("Too many invalid dates found. Stopping collection to prevent memory issues.");
                            break 2;
                        }
                    }
                }
                
                // Memory optimization every batchSize records
                if ($recordCount % $batchSize === 0) {
                    gc_collect_cycles();
                    ProgressDisplay::display("Processing $tableName", $recordCount, 0);
                }
            }
            
            $table->close();
            $this->totalRecordsProcessed += $recordCount;
            
            ProgressDisplay::info("Completed $tableName: $recordCount records processed, $invalidCount invalid dates found");
            
        } catch (Exception $e) {
            ProgressDisplay::error("Failed to process $tableName: " . $e->getMessage());
        }
    }
    
    /**
     * Generate comprehensive report
     */
    private function generateReport()
    {
        ProgressDisplay::complete("DBF Date Validation Complete!");
        
        echo "\n";
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
        echo "📊 VALIDATION SUMMARY\n";
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
        echo "Total Records Processed: " . number_format($this->totalRecordsProcessed) . "\n";
        echo "Total Invalid Dates Found: " . number_format($this->totalInvalidDates) . "\n";
        echo "Tables Processed: " . count($this->getDBFTables()) . "\n";
        
        if ($this->totalInvalidDates > 0) {
            echo "\n";
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
            echo "❌ INVALID DATES FOUND\n";
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
            
            // Group by table
            $groupedByTable = [];
            foreach ($this->invalidDates as $invalid) {
                $groupedByTable[$invalid['table']][] = $invalid;
            }
            
            foreach ($groupedByTable as $table => $invalidDates) {
                echo "\n📋 Table: $table (" . count($invalidDates) . " invalid dates)\n";
                echo str_repeat('-', 80) . "\n";
                
                // Group by field
                $groupedByField = [];
                foreach ($invalidDates as $invalid) {
                    $groupedByField[$invalid['field']][] = $invalid;
                }
                
                foreach ($groupedByField as $field => $fieldInvalidDates) {
                    echo "  🔸 Field: $field (" . count($fieldInvalidDates) . " invalid values)\n";
                    
                    // Show sample invalid values (limit to 10)
                    $sampleValues = array_slice($fieldInvalidDates, 0, 10);
                    foreach ($sampleValues as $invalid) {
                        echo "    - Record #{$invalid['record_number']}: '{$invalid['value']}'\n";
                    }
                    
                    if (count($fieldInvalidDates) > 10) {
                        echo "    ... and " . (count($fieldInvalidDates) - 10) . " more\n";
                    }
                }
            }
            
            // Save detailed report to file
            $this->saveDetailedReport();
            
        } else {
            echo "\n✅ No invalid dates found! All date fields are valid.\n";
        }
        
        echo "\n";
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    }
    
    /**
     * Save detailed report to CSV file
     */
    private function saveDetailedReport()
    {
        if (empty($this->invalidDates)) {
            return;
        }
        
        $reportFile = __DIR__ . '/cache/invalid_dates_report_' . date('Y-m-d_H-i-s') . '.csv';
        
        try {
            $fp = fopen($reportFile, 'w');
            
            // Write CSV header
            fputcsv($fp, ['Table', 'Field', 'Value', 'Record Number', 'Timestamp']);
            
            // Write data
            foreach ($this->invalidDates as $invalid) {
                fputcsv($fp, [
                    $invalid['table'],
                    $invalid['field'],
                    $invalid['value'],
                    $invalid['record_number'],
                    $invalid['timestamp']
                ]);
            }
            
            fclose($fp);
            
            ProgressDisplay::info("Detailed report saved to: $reportFile");
            
        } catch (Exception $e) {
            ProgressDisplay::error("Failed to save detailed report: " . $e->getMessage());
        }
    }
    
    /**
     * Main validation process
     */
    public function run()
    {
        // Initialize sync environment
        initializeSyncEnvironment();
        ProgressDisplay::start("🔍 Starting DBF Date Validation Process");
        
        try {
            $tables = $this->getDBFTables();
            $totalTables = count($tables);
            
            ProgressDisplay::info("Found $totalTables tables to validate: " . implode(', ', $tables));
            
            $processedTables = 0;
            
            foreach ($tables as $table) {
                $processedTables++;
                ProgressDisplay::display("Validating table $table", $processedTables, $totalTables);
                
                try {
                    $this->validateDBFFile($table);
                } catch (Exception $e) {
                    ProgressDisplay::error("Failed to validate $table: " . $e->getMessage());
                    continue;
                }
                
                // Memory cleanup between tables
                gc_collect_cycles();
            }
            
            // Generate final report
            $this->generateReport();
            
        } catch (Exception $e) {
            ProgressDisplay::error("Validation process failed: " . $e->getMessage());
            exit(1);
        }
    }
}

// Run the validator
$validator = new DBFDateValidator();
$validator->run();
