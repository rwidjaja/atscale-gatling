package executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryExtractExecutor extends com.atscale.java.executors.QueryExtractExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExtractExecutor.class);

    public static void main(String[] args) {
        QueryExtractExecutor executor = new QueryExtractExecutor();
        executor.execute();
    }
}
