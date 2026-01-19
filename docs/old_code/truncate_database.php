<?php
/**
 * Database Truncation Script
 * 
 * This script truncates all remote tables that receive UBS data.
 * Use this to clear remote data before a fresh sync.
 * 
 * Usage: php truncate_database.php [option]
 * Options:
 *   --info, -i     Show table information
 *   --truncate, -t Truncate all remote tables
 *   --help, -h     Show help
 *   (no option)    Interactive menu
 */

require_once 'bootstrap/app.php';
require_once 'includes/classes/mysql.class.php';

// Classes are loaded via require_once statements above

class DatabaseTruncator {
    private $mysql;
    private $tables;
    
    public function __construct() {
        $this->mysql = new MySQL();
        $this->mysql->connect_remote(); // Connect to remote database
        $this->tables = Converter::ubsTable();
    }
    
    /**
     * Get remote table names from UBS table names
     */
    private function getRemoteTables() {
        $remote_tables = [];
        foreach ($this->tables as $ubs_table) {
            $remote_table = Converter::table_convert_remote($ubs_table);
            if ($remote_table) {
                $remote_tables[] = $remote_table;
            }
        }
        return $remote_tables;
    }
    
    /**
     * Truncate all remote tables
     */
    public function truncateAllTables() {
        echo "🗑️  Database Truncation Script\n";
        echo "=====================================\n";
        echo "⚠️  WARNING: This will delete ALL data in remote tables!\n";
        echo "=====================================\n\n";
        
        // Get confirmation
        echo "Are you sure you want to truncate all tables? (yes/no): ";
        $handle = fopen("php://stdin", "r");
        $confirmation = trim(fgets($handle));
        fclose($handle);
        
        if (strtolower($confirmation) !== 'yes') {
            echo "❌ Operation cancelled.\n";
            return;
        }
        
        echo "\n🚀 Starting remote table truncation...\n\n";
        
        $truncated_count = 0;
        $error_count = 0;
        
        $remote_tables = $this->getRemoteTables();
        foreach ($remote_tables as $table_name) {
            
            try {
                // Truncate the table
                $sql = "TRUNCATE TABLE `{$table_name}`";
                $result = $this->mysql->query($sql);
                
                if ($result) {
                    echo "✅ Truncated: {$table_name}\n";
                    $truncated_count++;
                } else {
                    echo "❌ Failed to truncate: {$table_name} - " . $this->mysql->getLastError() . "\n";
                    $error_count++;
                }
                
            } catch (Exception $e) {
                echo "❌ Error truncating {$table_name}: " . $e->getMessage() . "\n";
                $error_count++;
            }
        }
        
        // Also truncate sync_logs table if it exists
        try {
            $sql = "TRUNCATE TABLE `sync_logs`";
            $result = $this->mysql->query($sql);
            
            if ($result) {
                echo "✅ Truncated: sync_logs\n";
                $truncated_count++;
            } else {
                echo "ℹ️  sync_logs table doesn't exist or couldn't be truncated\n";
            }
        } catch (Exception $e) {
            echo "ℹ️  sync_logs table doesn't exist or couldn't be truncated\n";
        }
        
        echo "\n=====================================\n";
        echo "📊 TRUNCATION SUMMARY\n";
        echo "=====================================\n";
        echo "✅ Tables truncated: {$truncated_count}\n";
        echo "❌ Errors: {$error_count}\n";
        echo "📋 Total remote tables processed: " . count($remote_tables) . "\n";
        
        if ($error_count === 0) {
            echo "\n🎉 All tables truncated successfully!\n";
            echo "💡 You can now run a fresh sync with: python main.py\n";
        } else {
            echo "\n⚠️  Some tables could not be truncated. Check the errors above.\n";
        }
        
        echo "=====================================\n";
    }
    
    /**
     * Show table information before truncation
     */
    public function showTableInfo() {
        echo "📋 Remote tables that will be truncated:\n";
        echo "=====================================\n";
        
        $remote_tables = $this->getRemoteTables();
        foreach ($remote_tables as $table_name) {
            
            // Get record count
            try {
                $sql = "SELECT COUNT(*) as count FROM `{$table_name}`";
                $result = $this->mysql->query($sql);
                $count = 0;
                
                if ($result && $result->num_rows > 0) {
                    $row = $result->fetch_assoc();
                    $count = $row['count'];
                }
                
                echo "📄 {$table_name} - {$count} records\n";
                
            } catch (Exception $e) {
                echo "📄 {$table_name} - Error getting count\n";
            }
        }
        
        echo "=====================================\n\n";
    }
    
    /**
     * Interactive menu
     */
    public function showMenu() {
        while (true) {
            echo "\n🗑️  Database Truncation Menu\n";
            echo "=====================================\n";
            echo "1. Show table information\n";
            echo "2. Truncate all tables\n";
            echo "3. Exit\n";
            echo "=====================================\n";
            echo "Choose an option (1-3): ";
            
            $handle = fopen("php://stdin", "r");
            $choice = trim(fgets($handle));
            fclose($handle);
            
            switch ($choice) {
                case '1':
                    $this->showTableInfo();
                    echo "\nPress Enter to continue...";
                    fgets(fopen("php://stdin", "r"));
                    break;
                case '2':
                    $this->truncateAllTables();
                    echo "\n✅ Truncation completed. Exiting...\n";
                    exit(0);
                case '3':
                    echo "👋 Goodbye!\n";
                    exit(0);
                default:
                    echo "❌ Invalid option. Please choose 1, 2, or 3.\n";
            }
        }
    }
}

// Main execution
if (php_sapi_name() === 'cli') {
    $truncator = new DatabaseTruncator();
    
    // Check if command line argument is provided
    if (isset($argv[1])) {
        switch ($argv[1]) {
            case '--info':
            case '-i':
                $truncator->showTableInfo();
                break;
            case '--truncate':
            case '-t':
                $truncator->truncateAllTables();
                break;
            case '--help':
            case '-h':
                echo "🗑️  Database Truncation Script\n";
                echo "Usage: php truncate_database.php [option]\n\n";
                echo "Options:\n";
                echo "  --info, -i     Show table information\n";
                echo "  --truncate, -t Truncate all remote tables\n";
                echo "  --help, -h     Show this help\n";
                echo "  --menu         Interactive menu\n";
                break;
            case '--menu':
                $truncator->showMenu();
                break;
            default:
                echo "❌ Unknown option: {$argv[1]}\n";
                echo "Use --help for usage information.\n";
        }
    } else {
        // No arguments provided - open interactive menu by default
        $truncator->showMenu();
    }
} else {
    echo "❌ This script must be run from the command line.\n";
    echo "Usage: php truncate_database.php\n";
}
?>
