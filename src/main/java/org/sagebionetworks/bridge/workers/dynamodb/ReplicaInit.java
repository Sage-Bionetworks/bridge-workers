package org.sagebionetworks.bridge.workers.dynamodb;

import org.sagebionetworks.bridge.config.Environment;

public class ReplicaInit {
    
    public ReplicaInit(final Environment env, final String user) {
        
        // Scan for "env-user-" tables
        // For tables that exist, compare schema
        // For tables that do not exist, create table
        
    }

}
