import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.util.Properties;

public class GatlingMain {
    
    // Exact mapping from your Python script
    private static final String[][] EXECUTOR_MAPPING = {
        {"InstallerVerQueryExtractExecutor", "executors.InstallerVerQueryExtractExecutor"},
        {"CustomQueryExtractExecutor", "executors.CustomQueryExtractExecutor"},
        {"QueryExtractExecutor", "executors.QueryExtractExecutor"},
        {"OpenStepConcurrentSimulationExecutor", "executors.OpenStepConcurrentSimulationExecutor"},
        {"ClosedStepConcurrentSimulationExecutor", "executors.ClosedStepConcurrentSimulationExecutor"},
        {"OpenStepSequentialSimulationExecutor", "executors.OpenStepSequentialSimulationExecutor"},
        {"ClosedStepSequentialSimulationExecutor", "executors.ClosedStepSequentialSimulationExecutor"},
        {"ArchiveJdbcToSnowflake", "executors.ArchiveJdbcToSnowflakeExecutor"},
        {"ArchiveXmlaToSnowflake", "executors.ArchiveXmlaToSnowflakeExecutor"}
    };
    
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }
        
        String executorName = args[0];
        String propertiesFile = args.length > 1 ? args[1] : "working_dir/config/systems.properties";
        
        try {
            // Load systems.properties and set as system properties
            Properties props = new Properties();
            props.load(new FileInputStream(propertiesFile));
            
            for (String key : props.stringPropertyNames()) {
                System.setProperty(key, props.getProperty(key));
            }
            
            // Run the selected executor
            runExecutor(executorName, args);
            
        } catch (Exception e) {
            System.err.println("Error running executor '" + executorName + "': " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void runExecutor(String executorName, String[] args) throws Exception {
        String className = findClassName(executorName);
        
        if (className == null) {
            System.err.println("Unknown executor: " + executorName);
            printAvailableExecutors();
            System.exit(1);
        }
        
        try {
            Class<?> executorClass = Class.forName(className);
            Method mainMethod = executorClass.getMethod("main", String[].class);
            
            // Create args without the executor name for the target main method
            String[] executorArgs = new String[args.length - 1];
            System.arraycopy(args, 1, executorArgs, 0, args.length - 1);
            
            System.out.println("Starting executor: " + executorName + " (class: " + className + ")");
            mainMethod.invoke(null, (Object) executorArgs);
            
        } catch (ClassNotFoundException e) {
            System.err.println("Executor class not found: " + className);
            System.err.println("Make sure the class is compiled and in the classpath");
            System.exit(1);
        } catch (NoSuchMethodException e) {
            System.err.println("Executor class doesn't have a main method: " + className);
            System.exit(1);
        }
    }
    
    private static String findClassName(String executorName) {
        for (String[] mapping : EXECUTOR_MAPPING) {
            if (mapping[0].equals(executorName)) {
                return mapping[1];
            }
        }
        return null;
    }
    
    private static void printAvailableExecutors() {
        System.err.println("Available executors:");
        for (String[] mapping : EXECUTOR_MAPPING) {
            System.err.println("  " + mapping[0]);
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: java -jar atscale-gatling.jar <executor> [systems.properties] [executor-args]");
        System.out.println();
        System.out.println("Available executors:");
        for (String[] mapping : EXECUTOR_MAPPING) {
            System.out.println("  " + mapping[0]);
        }
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar atscale-gatling.jar QueryExtractExecutor");
        System.out.println("  java -jar atscale-gatling.jar OpenStepConcurrentSimulationExecutor working_dir/config/systems.properties");
        System.out.println("  java -jar atscale-gatling.jar ArchiveJdbcToSnowflake /custom/path/systems.properties");
    }
}