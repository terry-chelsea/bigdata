package org.apache.kylin.client;

public class KylinClient {
    public static void main(String[] args) {
    	OptionsProcessor oproc = new OptionsProcessor();
    	Config config = oproc.precessArgs(args);
    	
    	if(config == null) {
    		System.exit(-1);
    	}
    	
    	int ret = new ClientDriver(config).run();
    	System.exit(ret);
    }
}
